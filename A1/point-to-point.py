from mpi4py import MPI

comm = MPI.COMM_WORLD

my_rank = comm.Get_rank()
p = comm.Get_size()
tag = 0


if my_rank != 0:
     message = f"Greeting from process {my_rank}"
     dest = 0
     comm.send(message, dest, tag)
else:
     for source in range(1, p):
          message = comm.recv(source=source, tag=tag)
          print(f"{message}")