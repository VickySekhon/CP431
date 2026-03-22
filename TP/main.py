import mpi4py
import matplotlib.pyplot as plt
import numpy as np
import os

print("Fractal generation!")


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

     def create_heatmap(self, data: np.ndarray, figsize: tuple[int, int]=(10,6)) -> None:
          plt.figure(figsize=figsize)
          plt.imshow(data, cmap="Reds")
          plt.colorbar(label="Intensity")
          plt.savefig(self.full_path)

class Fractal:
    CIRCLE_LOWER_BOUND = -2
    CIRCLE_UPPER_BOUND = 2
    ESCAPE_ITERATIONS = 50

    def __init__(self, dimension: int, c: complex):
        self.per_pixel_info = {}  # tracks bounded/unbounded behavior per pixel
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
                print(
                    f"Pixel: {pixel} escaped radius at {idx}th iteration. It was not added to Julia Set."
                )
                return idx
        print(f"Pixel: {pixel} remained within radius. It was added to Julia Set.")
        return escape_iterations

    def compute_pixel_info(self) -> dict[tuple, int]:
        per_pixel_info = {}
        for row in range(self.dimension):
            for col in range(self.dimension):
                pixel = (col, row)
                escape_iteration = self.test_if_pixel_bounded(pixel)
                per_pixel_info[pixel] = escape_iteration
        return per_pixel_info

    def set_pixel_info(self) -> None:
        self.per_pixel_info = self.compute_pixel_info()
        
    # Need conversion to plot escape information
    def convert_pixel_info_to_numpy(self) -> np.ndarray:
         if self.per_pixel_info == {}:
              self.set_pixel_info
         
         data = np.zeros((self.dimension, self.dimension))
         for (col, row) in self.per_pixel_info:
              value = self.per_pixel_info[(col, row)]
              data[row, col] = value
         
         return data
        
def main():
    fractal = Fractal(600, complex(-0.8, 0.156))
    figure = SaveFigure(os.path.join(os.curdir, "plots"), "heatmap.png")
    
    fractal.set_pixel_info()
    print(fractal.per_pixel_info)
    
    data = fractal.convert_pixel_info_to_numpy()
    figure.create_heatmap(data)
    
    return


if __name__ == "__main__":
    main()