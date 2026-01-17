from mpi4py import MPI
import sys

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()
group = comm.Get_group()
processor = MPI.Get_processor_name()
print(f"Hello from process {rank} of {size}, group {group} on machine {processor}")