from mpi4py import MPI
import math

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

N = 10**9

# Step 1: Rank 0 computes primes up to sqrt(N)
def simple_sieve(limit):
    sieve = [True] * (limit + 1)
    sieve[0:2] = [False, False]
    for i in range(2, int(math.sqrt(limit)) + 1):
        if sieve[i]:
            for j in range(i*i, limit + 1, i):
                sieve[j] = False
    return [i for i, is_p in enumerate(sieve) if is_p]

if rank == 0:
    base_primes = simple_sieve(int(math.sqrt(N)))
else:
    base_primes = None

base_primes = comm.bcast(base_primes, root=0)

# Step 2: Divide range
chunk = N // size
low = rank * chunk
high = N if rank == size - 1 else low + chunk
if low < 2:
    low = 2

segment_size = high - low
is_prime = [True] * segment_size

# Step 3: Segmented sieve
for p in base_primes:
    start = max(p*p, ((low + p - 1) // p) * p)
    for j in range(start, high, p):
        is_prime[j - low] = False

# Step 4: Find local prime gaps
prev = None
local_max_gap = 0
local_pair = (None, None)
first_prime = None
last_prime = None

for i in range(segment_size):
    if is_prime[i]:
        curr = low + i
        if prev is not None:
            gap = curr - prev
            if gap > local_max_gap:
                local_max_gap = gap
                local_pair = (prev, curr)
        else:
            first_prime = curr
        prev = curr

last_prime = prev

local_data = (local_max_gap, local_pair, first_prime, last_prime)
all_data = comm.gather(local_data, root=0)

# Step 5: Global reduction
if rank == 0:
    global_gap = 0
    best_pair = None

    # Local gaps
    for gap, pair, _, _ in all_data:
        if gap > global_gap:
            global_gap = gap
            best_pair = pair

    # Boundary gaps
    for i in range(len(all_data) - 1):
        last_p = all_data[i][3]
        first_p = all_data[i + 1][2]
        if last_p and first_p:
            gap = first_p - last_p
            if gap > global_gap:
                global_gap = gap
                best_pair = (last_p, first_p)

    print("Largest prime gap up to 1e9:", global_gap)
    print("Primes:", best_pair)
