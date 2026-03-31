from mpi4py import MPI
from OpenGL.GL import *
import pygame as pg
from pygame.locals import *
import matplotlib.pyplot as plt
import numpy as np
import os, argparse, math

"""
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

When you zoom: you are recomputing, not enlarging

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

    def create_pixel_csv(
        self, all_pixels: np.ndarray, c: complex
    ) -> None:
        # Strip the brackets
        c = str(c)[1:-1]
        np.save(f"julia_{c}.npy", all_pixels)


class Fractal:
    CIRCLE_LOWER_BOUND = -2
    CIRCLE_UPPER_BOUND = 2
    ESCAPE_ITERATIONS = 50

    def __init__(self, dimension: int, c: complex):
        # Tracks iteration pixels escaped circle bounded at origin of complex plane
        self.per_pixel_info = None
        self.dimension = dimension
        self.c = c

        self.pixels = dimension * dimension
        self._step_size = (
            self.CIRCLE_UPPER_BOUND - self.CIRCLE_LOWER_BOUND
        ) / dimension

    # Changes as we iterate
    def calculate_z_value_from_pixel_coordinates(self, pixel: tuple[int]) -> complex:
        i, j = pixel
        x = self.CIRCLE_LOWER_BOUND + i * self._step_size
        y = self.CIRCLE_LOWER_BOUND + j * self._step_size
        z = complex(x, y)
        return z

    def calculate_updated_z_value(self, z: complex) -> complex:
        z = z**2 + self.c
        return z

    def test_if_pixel_bounded(self, pixel: tuple[int], escape_iterations=None) -> int:
        if escape_iterations is None:
            escape_iterations = self.ESCAPE_ITERATIONS

        z = self.calculate_z_value_from_pixel_coordinates(pixel)
        for idx in range(escape_iterations):
            z = self.calculate_updated_z_value(z)
            if abs(z) > 2:
                # Pixel escaped radius at some iteration (not part of the Julia Set)
                return idx
        # Pixel remained within radius (part of the Julia Set)
        return escape_iterations

    def compute_pixel_info(self, row_start, row_end) -> np.ndarray:
        n_W = row_end - row_start
        data = np.zeros((n_W, self.dimension), dtype=np.uint8)
        # Each process computes pixel info for a subset of rows
        for row in range(row_start, row_end):
            for col in range(self.dimension):
                pixel = (col, row)
                data[row - row_start, col] = self.test_if_pixel_bounded(pixel)
        return data

    def set_pixel_info(self, row_start, row_end) -> None:
        self.per_pixel_info = self.compute_pixel_info(row_start, row_end)


def main():
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
        fractal.set_pixel_info(row_start, row_end)
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

class Renderer:
    def __init__(self):
        self.pixel_map = None
        self.texture_id = None
    
    def load_pixel_map(self, npy_file_path):
        pixel_map = np.load(npy_file_path)
        normalized_pixel_map = self.normalize_pixel_values(pixel_map)
        self.set_pixel_map(normalized_pixel_map)
        return pixel_map
    
    def create_texture(self):
        assert self.pixel_map is not None, "Pixel map is empty, cannot create a texture"
        
        h, w = self.pixel_map.shape
        texture_id = glGenTextures(1)
        glBindTexture(GL_TEXTURE_2D, texture_id)
        
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_CLAMP_TO_EDGE)
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_CLAMP_TO_EDGE)
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR)
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR)
        
        params = {
            "target": GL_TEXTURE_2D,
            "level": 0,
            "internalformat": GL_RED,
            "width": w,
            "height": h,
            "border": 0,
            "format": GL_RED,
            "type": GL_UNSIGNED_BYTE,
            "data": self.pixel_map,
        }
        glTexImage2D(**params)
        self.texture_id = texture_id
    
    def draw_fractal(self):
        glClear(GL_COLOR_BUFFER_BIT)
        glEnable(GL_TEXTURE_2D)
        glBindTexture(GL_TEXTURE_2D, self.texture_id)
        
        glBegin(GL_QUADS)
        glTexCoord2f(0,0); glVertex2f(-1,-1)
        glTexCoord2f(0,1); glVertex2f(-1,1)
        glTexCoord2f(1,0); glVertex2f(1,-1)
        glTexCoord2f(1,1); glVertex2f(1,1)
        glEnd()
        
        glDisable(GL_TEXTURE_2D)
    
    # Scale escape counts to full RGB range
    def normalize_pixel_values(self, pixel_map):
        return (pixel_map / 50 * 255).astype(np.uint8)
    
    def set_pixel_map(self, pixel_map):
        self.pixel_map = pixel_map

if __name__ == "__main__":
    # main()
    pg.init()
    pg.display.set_mode((900, 900), DOUBLEBUF | OPENGL)
    
    renderer = Renderer()
    renderer.load_pixel_map("./julia-sets/julia_-0.1+0.8j.npy")
    renderer.create_texture()
    
    running = True
    while running:
        for event in pg.event.get():
            if event.type == pg.QUIT:
                running = False
        renderer.draw_fractal()
    
    pg.quit()
        
    


"""
Julia Set is exported, now I need to render it with OpenGL

first copy over the npy arrays from cluster to local using scp

You'll need two libraries:
pip install PyOpenGL PyOpenGL_accelerate Pillow numpy

The Core Approach
Load your .npy file, convert it to a texture, and render it on a flat quad that fills the screen. The steps are:

Load the data: np.load("julia.npy")
Normalize to 0–255: scale escape counts to full color range
Apply a colormap: map the grayscale values to RGB colors
Upload as an OpenGL texture: glTexImage2D
Render a fullscreen quad: two triangles that fill the window
"""