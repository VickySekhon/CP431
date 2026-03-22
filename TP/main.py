from mpi4py import MPI
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


class Fractal:
    CIRCLE_LOWER_BOUND = -2
    CIRCLE_UPPER_BOUND = 2
    ESCAPE_ITERATIONS = 50

    def __init__(self, dimension: int, c: complex):
        self.per_pixel_info = {}  # Tracks bounded/unbounded behavior per pixel
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

    def compute_pixel_info(self, row_start, row_end) -> dict[tuple, int]:
        per_pixel_info = {}
        # Each process computes pixel info for a subset of rows
        for row in range(row_start, row_end):
            for col in range(self.dimension):
                pixel = (col, row)
                escape_iteration = self.test_if_pixel_bounded(pixel)
                per_pixel_info[pixel] = escape_iteration
        return per_pixel_info

    def set_pixel_info(self, row_start, row_end) -> None:
        self.per_pixel_info = self.compute_pixel_info(row_start, row_end)

    # Need conversion to plot escape information
    def convert_pixel_info_to_numpy(self, n_W, row_start) -> np.ndarray:
        assert (
            len(self.per_pixel_info) > 0
        ), "per_pixel_info is empty cannot convert it to numPy"
        # Only need a subset of rows per processor
        pixel_info = np.zeros((n_W, self.dimension))
        for col, row in self.per_pixel_info:
            value = self.per_pixel_info[(col, row)]
            local_row = row - row_start
            pixel_info[local_row, col] = value
        return pixel_info


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
        subset = fractal.convert_pixel_info_to_numpy(n_W, row_start)
        message = (subset, row_start, row_end)

        comm.send(message, destination)
    else:
        all_pixels = np.zeros((fractal.dimension, fractal.dimension))
        for _ in range(1, P):
            message = comm.recv(source=MPI.ANY_SOURCE)
            subset, row_start, row_end = message

            all_pixels[row_start:row_end, :] = subset
        # print(f"Computed fractal (pixel representation): {all_pixels}")

        figure = SaveFigure(os.path.join(os.curdir, "plots"), "heatmap.png")
        figure.create_heatmap(all_pixels)


if __name__ == "__main__":
    main()
