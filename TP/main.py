from mpi4py import MPI
from OpenGL.GL import *
import pygame as pg
from pygame.locals import *
import matplotlib.pyplot as plt
import matplotlib.cm as cm
import numpy as np
import os, argparse, math

"""
My notes are below. They provide insight into my thoughts during the implementation of this project:

complex number: z = a + bi
where a and b are real numbers
and i is the imaginary part √-1

10,000 × 10,000 = 100,000,000 pixels

step = 4 / 10000 = 0.0004

each pixel (i,j) would map to:
x = -2 + i * 0.004
y = -2 + j * 0.004
z = x + yj

z = z^2 + c, where z and c are complex numbers

z corresponds to pixel you are currently testing while c is a fixed constant

and run for 50 iterations
to see if it's bounded (within 
circle of radius 2 from origin)
c stays constant the entire time

different values of c produce different julia sets

coloring = within 50 iterations the points that escape 
early are one color,points that escape later are another
color, and points that never escape are black 

To get infinite zoom capabilities, OpenGL recomputes z
in real-time.
CPU:
    compute all pixels → store in dict/array → display
GPU (per pixel):
    for each screen pixel:
        compute z = f(z) in real time

When you zoom you are recomputing, not enlarging

Julia Set is exported, now I need to render it with OpenGL

first copy over the npy arrays from cluster to local using scp

Load the data: np.load("julia.npy")
Normalize to 0–255: scale escape counts to full color range
Apply a colormap: map the grayscale values to RGB colors
Upload as an OpenGL texture: glTexImage2D
Render a fullscreen quad: two triangles that fill the window
"""


class SaveFigure:
    def __init__(self, directory, file_name):
        self.directory = directory
        self.filename = file_name
        self.full_path = os.path.join(directory, file_name)

        os.makedirs(directory, exist_ok=True)

    def create_heatmap(
        self, data: np.ndarray, figsize: tuple[int, int] = (10, 6)
    ) -> None:
        plt.figure(figsize=figsize)
        plt.imshow(data, cmap="Reds")
        plt.colorbar(label="Intensity")
        plt.savefig(self.full_path)

    def create_pixel_csv(self, all_pixels: np.ndarray, c: complex) -> None:
        # Strip the brackets
        c = str(c)[1:-1]
        np.save(f"julia_{c}.npy", all_pixels)


class Fractal:
    ESCAPE_ITERATIONS = 50

    def __init__(self, dimension: int, c: complex, x_min, x_max, y_min, y_max):
        # Tracks iteration pixels escaped circle bounded at origin of complex plane
        self.per_pixel_info = None
        self.dimension = dimension
        self.pixels = dimension * dimension
        self.c = c

        # Complex plane coordinates
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max

        # Step size within our complex plane
        self._step_x = (x_max - x_min) / dimension
        self._step_y = (y_max - y_min) / dimension

    # Z is a point on our complex plane
    def calculate_z_value_from_pixel_coordinates(self, pixel: tuple[int]) -> complex:
        col, row = pixel
        x = self.x_min + col * self._step_x
        y = self.y_min + row * self._step_y
        z = complex(x, y)
        return z

    def calculate_updated_z_value(self, z: complex) -> complex:
        z = z**2 + self.c
        return z

    def test_if_pixel_bounded(self, pixel: tuple[int]) -> int:
        z = self.calculate_z_value_from_pixel_coordinates(pixel)

        for idx in range(self.ESCAPE_ITERATIONS):
            z = self.calculate_updated_z_value(z)
            if abs(z) > 2:
                # Pixel escaped radius at some iteration (not part of the Julia Set)
                return idx
        # Pixel remained within radius (part of the Julia Set)
        return self.ESCAPE_ITERATIONS

    def compute_pixel_info(self, row_start, row_end) -> np.ndarray:
        row_count = row_end - row_start

        pixel_info_subset = np.zeros((row_count, self.dimension), dtype=np.uint8)
        # Each process computes pixel info for a subset of rows
        for row in range(row_start, row_end):
            for col in range(self.dimension):
                pixel = (col, row)
                # Relative index
                row_index = row - row_start
                pixel_info_subset[row_index, col] = self.test_if_pixel_bounded(pixel)
        return pixel_info_subset

    def set_pixel_info_parallel(self, row_start, row_end) -> None:
        self.per_pixel_info = self.compute_pixel_info(row_start, row_end)

    # Used for quick recomputation of fractal values
    # O(n) instead of O(n^2) in compute_pixel_info()
    def compute_pixel_info_vectorized(self) -> np.ndarray:
        dimension = np.arange(self.dimension)
        cols, rows = np.meshgrid(dimension, dimension)
        x = self.x_min + cols * self._step_x
        y = self.y_min + rows * self._step_y

        # Compute all z values together
        z = x + 1j * y

        # Default to escape at maximum iterations and update accordingly
        pixel_info = np.full(z.shape, self.ESCAPE_ITERATIONS, dtype=np.uint8)
        for idx in range(self.ESCAPE_ITERATIONS):
            # All bounded points
            mask = np.abs(z) <= 2
            # Updated bounded points
            z[mask] = z[mask] ** 2 + self.c
            # Points that were bounded but escaped are updated
            pixel_info[mask & (np.abs(z) > 2)] = idx
        return pixel_info

    def set_per_pixel_info(self, per_pixel_info):
        self.per_pixel_info = per_pixel_info


class Renderer:
    RGB_RANGE = 255

    def __init__(self):
        self.pixel_map = None
        self.texture_id = None

    # Allows for use of a smaller pixel map without having to recompute one
    def _reduce_2d_array_by_factor(self, array: np.ndarray, reduce_factor: int):
        return array[::reduce_factor, ::reduce_factor]

    def load_pixel_map(self, npy_file_path: str, reduce_factor: int):
        pixel_map = np.load(npy_file_path)
        if reduce_factor > 1:
            # Cannot run huge dimensions locally
            pixel_map = self._reduce_2d_array_by_factor(pixel_map, reduce_factor)

        pixel_map_norm = self.normalize_pixel_values(pixel_map)
        return pixel_map_norm

    def normalize_pixel_values(self, pixel_map: np.ndarray):
        pixel_map_norm = pixel_map / Fractal.ESCAPE_ITERATIONS
        # Convert escape counts to RGB for rendering
        # Apply a colormap based on escape counts
        # Each pixel value will be converted to RGBA
        # (e.g. [0.43..] -> [9.74638e-01, 7.97692e-01, 2.06332e-01, 1.00000e+00])
        colors = (cm.inferno(pixel_map_norm) * self.RGB_RANGE).astype(np.uint8)
        # Colormap returns a 3D array and we need to truncate RGBA to RGB
        return colors[:, :, :3]

    def create_texture(self, pixel_map):
        assert pixel_map is not None, "Pixel map is empty, cannot create a texture"

        height, width, _ = pixel_map.shape
        texture_id = glGenTextures(1)
        glBindTexture(GL_TEXTURE_2D, texture_id)

        # Texture is bigger than the pygame window. Select only the closest points to each window pixel
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_NEAREST)

        glTexImage2D(
            GL_TEXTURE_2D,
            0,
            GL_RGB,
            width,
            height,
            0,
            GL_RGB,
            GL_UNSIGNED_BYTE,
            pixel_map,
        )
        self.texture_id = texture_id

    def draw_fractal(self):
        glClear(GL_COLOR_BUFFER_BIT)
        glEnable(GL_TEXTURE_2D)
        glBindTexture(GL_TEXTURE_2D, self.texture_id)

        glBegin(GL_QUADS)
        glTexCoord2f(0.0, 0.0)
        glVertex2f(-1, -1)
        glTexCoord2f(1.0, 0.0)
        glVertex2f(1, -1)
        glTexCoord2f(1.0, 1.0)
        glVertex2f(1, 1)
        glTexCoord2f(0.0, 1.0)
        glVertex2f(-1, 1)
        glEnd()

        glDisable(GL_TEXTURE_2D)

    def recompute_fractal(self, fractal: Fractal):
        per_pixel_info = fractal.compute_pixel_info_vectorized()
        fractal.set_per_pixel_info(per_pixel_info)
        pixel_info_norm = self.normalize_pixel_values(fractal.per_pixel_info)
        self.create_texture(pixel_info_norm)


def compute_fractal():
    parser = argparse.ArgumentParser(
        description=(
            "Generate a Julia Set for a given constant 'c' and some dimension 'dim'"
        )
    )

    parser.add_argument(
        "c",
        type=complex,
        help="Some complex number 'c' that follows the format (a, b) which is the same as 'a + bi'",
    )
    parser.add_argument(
        "dim",
        type=int,
        help="Dimensions of the image Julia Set to generate (e.g. dim=600 will use 600x600 pixels)",
    )

    args = parser.parse_args()
    c = args.c
    n = args.dim

    fractal = Fractal(n, c)

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    P = comm.Get_size()
    # Rank 0 is reserved for collection
    # Leaves us with Ranks 1 to P-1 which are workers
    # These are pseudo ranks, underlying ranks are still needed for process identification
    W = P - 1

    if rank != 0:
        destination = 0
        # Start at w_rank 0 otherwise we'll skip rows
        w_rank = rank - 1
        n_W = math.floor(n / W)
        if w_rank < (n % W):
            n_W += 1
        i_start_rank = w_rank * math.floor(n / W) + min(w_rank, (n % W))

        row_start, row_end = i_start_rank, i_start_rank + n_W
        fractal.set_pixel_info_parallel(row_start, row_end)
        subset = fractal.per_pixel_info
        message = (subset, row_start, row_end)

        comm.send(message, destination)
    else:
        all_pixels = np.zeros((fractal.dimension, fractal.dimension), dtype=np.uint8)
        for _ in range(1, P):
            message = comm.recv(source=MPI.ANY_SOURCE)
            subset, row_start, row_end = message

            all_pixels[row_start:row_end, :] = subset
        # print(f"Computed fractal (pixel representation): {all_pixels}")

        figure = SaveFigure(
            os.path.join(os.curdir, "plots"), f"julia_{str(c)[1:-1]}.png"
        )
        figure.create_pixel_csv(all_pixels, c)
        figure.create_heatmap(all_pixels)


def render_fractal():
    parser = argparse.ArgumentParser(
        description=(
            "Render a Julia Set for a given constant 'c', some dimension 'dim', from 'path'"
        )
    )

    parser.add_argument(
        "c",
        type=complex,
        help="Some complex number 'c' that follows the format (a, b) which is the same as 'a + bi'",
    )
    parser.add_argument(
        "dim",
        type=int,
        help="Dimensions of the image Julia Set to generate (e.g. dim=600 will use 600x600 pixels)",
    )
    parser.add_argument(
        "path",
        type=str,
        help="Path to .npy file that stores the per-pixel escape counts of the fractal to render.",
    )

    args = parser.parse_args()
    path = args.path
    c = args.c
    dimension = args.dim

    # When recomputing fractal, only process a max of 2,250,000 pixel updates opposed to hundreds of millions
    DIMENSION_UPPER_LIMIT = 1200
    REDUCE_FACTOR = 4
    PYGAME_WINDOW_WIDTH, PYGAME_WINDOW_HEIGHT = 1000, 1000

    pg.init()
    pg.display.set_mode((PYGAME_WINDOW_WIDTH, PYGAME_WINDOW_HEIGHT), DOUBLEBUF | OPENGL)

    renderer = Renderer()
    pixel_map = renderer.load_pixel_map(path, REDUCE_FACTOR)
    renderer.create_texture(pixel_map)

    # Complex plane begins at -2 to 2
    x_min, x_max = -2.0, 2.0
    y_min, y_max = -2.0, 2.0
    ZOOM_IN, ZOOM_OUT = 0.5, 2

    mouse_x, mouse_y = 0, 0

    dragging = False
    running = True

    while running:
        curr_x, curr_y = pg.mouse.get_pos()
        if curr_x != mouse_x or curr_y != mouse_y:
            mouse_x = curr_x
            mouse_y = curr_y
            print(f"current position, x: {mouse_x}, y: {mouse_y}")
        for event in pg.event.get():
            if event.type == pg.QUIT:
                running = False
            elif event.type == pg.MOUSEWHEEL:
                if event.y > 0:
                    zoom_factor = ZOOM_IN
                    dimension = min(dimension + 100, DIMENSION_UPPER_LIMIT)
                elif event.y < 0:
                    zoom_factor = ZOOM_OUT

                mouse_x, mouse_y = pg.mouse.get_pos()
                norm_x = mouse_x / PYGAME_WINDOW_WIDTH
                # y=0 is the top of a pygame window while we treat it as the bottom
                norm_y = 1.0 - (mouse_y / PYGAME_WINDOW_HEIGHT)

                complex_x = x_min + norm_x * (x_max - x_min)
                complex_y = y_min + norm_y * (y_max - y_min)

                width = (x_max - x_min) * zoom_factor
                height = (y_max - y_min) * zoom_factor

                # New complex plane coordinates based on how far we zoomed into the window
                x_min, x_max = complex_x - width / 2, complex_x + width / 2
                y_min, y_max = complex_y - height / 2, complex_y + height / 2

                fractal = Fractal(dimension, c, x_min, x_max, y_min, y_max)
                renderer.recompute_fractal(fractal)
            elif event.type == pg.MOUSEBUTTONDOWN:
                if event.button == 1:
                    dragging = True
            elif event.type == pg.MOUSEBUTTONUP:
                if event.button == 1:
                    dragging = False
            elif event.type == pg.MOUSEMOTION:
                if dragging:
                    # Movement since last event
                    delta_x, delta_y = event.rel

                    width = x_max - x_min
                    height = y_max - y_min

                    # Moves the complex window opposite the mouse
                    delta_complex_x = -delta_x / PYGAME_WINDOW_WIDTH * width
                    delta_complex_y = delta_y / PYGAME_WINDOW_HEIGHT * height

                    x_min += delta_complex_x
                    x_max += delta_complex_x
                    y_min += delta_complex_y
                    y_max += delta_complex_y
                    fractal = Fractal(dimension, c, x_min, x_max, y_min, y_max)
                    renderer.recompute_fractal(fractal)
        renderer.draw_fractal()
        pg.display.flip()
    pg.quit()


if __name__ == "__main__":
    # compute_fractal
    render_fractal()
