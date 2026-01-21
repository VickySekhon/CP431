from mpi4py import MPI
from gmpy2 import mpz, next_prime

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

comm.Barrier()
start_time = MPI.Wtime()

# Change this to 10**9 or 10**12 (Do not run unless overnight if doing a trillion)
N = 10**9

# Step 1: Divide the range among ranks
chunk = N // size
low = rank * chunk
high = N if rank == size - 1 else low + chunk
if low < 2:
    low = 2

# Step 2: Find primes in your segment using GMP
prev_prime = None
local_max_gap = 0
local_pair = (None, None)
first_prime = None
last_prime = None

p = next_prime(mpz(low - 1))  # first prime >= low

while p <= high:
    if prev_prime is not None:
        gap = int(p - prev_prime)
        if gap > local_max_gap:
            local_max_gap = gap
            local_pair = (int(prev_prime), int(p))
    else:
        first_prime = int(p)
    prev_prime = mpz(p)
    last_prime = int(p)
    p = next_prime(p)

# Step 3: Gather local results to rank 0
local_data = (local_max_gap, local_pair, first_prime, last_prime)
all_data = comm.gather(local_data, root=0)

# Step 4: Rank 0 computes global max gap
if rank == 0:
    global_gap = 0
    best_pair = None

    # Check local gaps
    for gap, pair, _, _ in all_data:
        if gap > global_gap:
            global_gap = gap
            best_pair = pair

    # Check boundary gaps between ranks
    for i in range(len(all_data) - 1):
        last_p = all_data[i][3]
        first_p = all_data[i + 1][2]
        if last_p and first_p:
            gap = first_p - last_p
            if gap > global_gap:
                global_gap = gap
                best_pair = (last_p, first_p)

    end_time = MPI.Wtime()
    elapsed = end_time - start_time

    print(f"Processes: {size}")
    print(f"Wall time (s): {elapsed:.2f}")
    print(f"Largest prime gap up to {N}: {global_gap}")
    print(f"Primes: {best_pair}")
