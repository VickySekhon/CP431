from mpi4py import MPI
from OpenGL.GL import *
import pygame as pg
from pygame.locals import *
import matplotlib.pyplot as plt
import matplotlib.cm as cm
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

    def __init__(self, dimension: int, c: complex, x_min, x_max, y_min, y_max):
        # Tracks iteration pixels escaped circle bounded at origin of complex plane
        self.per_pixel_info = None
        self.dimension = dimension
        self.pixels = dimension * dimension
        self.c = c
        
        self.x_min = x_min
        self.x_max = x_max
        self.y_min = y_min
        self.y_max = y_max

        self._step_x =  (x_max - x_min) / dimension
        self._step_y =  (y_max - y_min) / dimension

    # Changes as we iterate
    def calculate_z_value_from_pixel_coordinates(self, pixel: tuple[int]) -> complex:
        col, row = pixel
        x = self.x_min + col * self._step_x
        y = self.y_min + row * self._step_y
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
        
    def compute_pixel_info_vectorized(self):
        cols, rows = np.meshgrid(np.arange(self.dimension), np.arange(self.dimension))
        x = self.x_min + cols * self._step_x
        y = self.y_min + rows * self._step_y
        z = x + 1j * y
        escape = np.full(z.shape, self.ESCAPE_ITERATIONS, dtype=np.uint8)
        for idx in range(self.ESCAPE_ITERATIONS):
            mask = np.abs(z) <= 2
            z[mask] = z[mask]**2 + self.c
            escape[mask & (np.abs(z) > 2)] = idx
        self.per_pixel_info = escape
        #return escape

class Renderer:
    RGB_RANGE = 255
    
    def __init__(self):
        self.pixel_map = None
        self.texture_id = None
    
    def _reduce_2d_array_by_factor(self, array: np.ndarray, factor: int):
        return array[::factor, ::factor]
    
    def load_pixel_map(self, npy_file_path: str, downsample: int=1):
        pixel_map = np.load(npy_file_path)
        if downsample > 1:
            # Cannot run huge dimensions locally
            pixel_map = self._reduce_2d_array_by_factor(pixel_map, downsample)
        print(pixel_map)
        normalized = self.normalize_pixel_values(pixel_map)
        self.set_pixel_map(normalized)
        
    def create_texture(self):
        assert self.pixel_map is not None, "Pixel map is empty, cannot create a texture"
        
        h, w, _ = self.pixel_map.shape
        texture_id = glGenTextures(1)
        glBindTexture(GL_TEXTURE_2D, texture_id)
        
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_CLAMP_TO_EDGE)
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_CLAMP_TO_EDGE)
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR)
        glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR)
        
        glTexImage2D(
            GL_TEXTURE_2D,      # target
            0,                  # level
            GL_RGB,             # internalformat
            w,                  # width
            h,                  # height
            0,                  # border
            GL_RGB,             # format
            GL_UNSIGNED_BYTE,   # type
            self.pixel_map      # data
        )
        self.texture_id = texture_id
    
    def draw_fractal(self):
        glClear(GL_COLOR_BUFFER_BIT)
        glEnable(GL_TEXTURE_2D)
        glBindTexture(GL_TEXTURE_2D, self.texture_id)

        glBegin(GL_QUADS)
        glTexCoord2f(0.0, 0.0); glVertex2f(-1, -1)
        glTexCoord2f(1.0, 0.0); glVertex2f( 1, -1)
        glTexCoord2f(1.0, 1.0); glVertex2f( 1,  1)
        glTexCoord2f(0.0, 1.0); glVertex2f(-1,  1)
        glEnd()

        glDisable(GL_TEXTURE_2D)
    
    # Scale escape counts to full RGB range
    def normalize_pixel_values(self, pixel_map: np.ndarray):
        normalized = pixel_map / Fractal.ESCAPE_ITERATIONS
        # Apply a colormap based on escape counts
        # Each pixel value will be converted to RGBA 
        # (e.g. [0.43..] -> [9.74638e-01, 7.97692e-01, 2.06332e-01, 1.00000e+00])
        colors = (cm.inferno(normalized) * self.RGB_RANGE).astype(np.uint8)
        # Colormap returns a 3D array and we truncate RGBA to RGB
        return colors[:, :, :3]
    
    def recompute(self, fractal: Fractal, row_start=0, row_end=500):
        fractal.compute_pixel_info_vectorized()
        pixel_info_norm = self.normalize_pixel_values(fractal.per_pixel_info)
        self.set_pixel_map(pixel_info_norm)
        self.create_texture()
    
    def set_pixel_map(self, pixel_map: np.ndarray):
        self.pixel_map = pixel_map

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
    #fractal = Fractal(n, c)

    # We have a 30,000 x 30,000 pixel map, that needs to be converted to dimension of 'n'
    #downsample = int((30_000 * 30_000) / (n*n))
    downsample = 4
    
    pg.init()
    pg.display.set_mode((900, 900), DOUBLEBUF | OPENGL)
    
    renderer = Renderer()
    renderer.load_pixel_map("./julia-sets/julia_-0.1+0.8j.npy", downsample)
    renderer.create_texture()
    
    x_min, x_max = -2.0, 2.0
    y_min, y_max = -2.0, 2.0
    ZOOM_IN = 0.5     # shrink window
    ZOOM_OUT = 2      # expand window
    running = True
    # c = complex(-0.1, 0.8)
    mouse_x, mouse_y = 0, 0
    
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
                    n = min(n + 100, 1500)
                elif event.y < 0:
                    zoom_factor = ZOOM_OUT
                    n = max(n - 100, 500)
                
                mouse_x, mouse_y = pg.mouse.get_pos()
                size_width, size_height = pg.display.get_surface().get_size()
                print(f"current size: {pg.display.get_surface().get_size()}")
                # 0-1
                norm_x = mouse_x / size_width
                norm_y = 1.0 - (mouse_y / size_height)
                
                complex_x = x_min + norm_x * (x_max - x_min)
                complex_y = y_min + norm_y * (y_max - y_min)
                
                width = (x_max - x_min) * zoom_factor
                height = (y_max - y_min) * zoom_factor
                x_min, x_max = complex_x - width/2, complex_x + width/2
                y_min, y_max = complex_y - height/2, complex_y + height/2
                
                fractal = Fractal(n, c, x_min, x_max, y_min, y_max)
                renderer.recompute(fractal)
        renderer.draw_fractal()
        pg.display.flip()
    pg.quit()


if __name__ == "__main__":
    main()
        
    


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