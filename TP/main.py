import mpi4py

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
"""

class Fractal:
     CIRCLE_LOWER_BOUND = -2
     CIRCLE_UPPER_BOUND = 2
     ESCAPE_ITERATIONS = 50
     
     def __init__(self, dimension: int, c: complex):
          self.per_pixel_info = {} # tracks bounded/unbounded behavior per pixel
          self.dimension = dimension
          self.c = c
          
          self.pixels = dimension*dimension
          self._step_size = (self.CIRCLE_UPPER_BOUND - self.CIRCLE_LOWER_BOUND) / dimension

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
                    print(f"Pixel: {pixel} escaped radius at {idx}th iteration. It was not added to Julia Set.")
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
     
def main():
     fractal = Fractal(10, complex(-0.8, 0.156))
     fractal.set_pixel_info()
     print(fractal.per_pixel_info)
     return

if __name__ == "__main__":
     main()
