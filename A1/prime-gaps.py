import math
from mpi4py import MPI

# determine if a number is prime
def is_prime(n):     
     if n < 2: return False
     
     upper_bound = math.floor(math.sqrt(n)) + 1 # account for "range" 
     for i in range(2, upper_bound):
          if n % i == 0:
               return False
     return True

# find prime numbers up to 'n'
def find_primes(n):
     primes = []
     for i in range(1,n+1):
          if is_prime(i): primes.append(i)
     return primes

def compute_max_gap(primes):     
     n = len(primes)
     
     # keep track of a max gap variable
     max_gap = 0
     a, b = None, None
     
     # iterate through prime numbers
     for i, curr in enumerate(primes):
          if i + 1 < n:
               next_ = primes[i+1]
               if next_ - curr > max_gap:
                    max_gap = next_ - curr
                    a, b = curr, next_
     
     return max_gap, a, b

comm = MPI.COMM_WORLD

rank = comm.Get_rank()
P = comm.Get_size()

W = P-1
n = 1000000000
primes = find_primes(n)

if rank != 0:
     dest = 0
     w_id = rank-1
     #n_p = math.floor(n/W) + 1 if w_id < n%W else 0
     n_p = math.floor(n/W)
     if w_id < n%W:
          n_p + 1
     i_start_p = (w_id*(math.floor(n/W))) + min(w_id, n%W)
     gap, a, b = compute_max_gap(primes[i_start_p:i_start_p+n_p])
     message = (gap, a, b)
     comm.send(message, dest)
else:
     max_gap = 0
     max_a, max_b = None, None
     for i in range(1, P):
          message = comm.recv(source=i)
          gap, a, b = message
          if gap > max_gap:
               max_gap = gap
               max_a, max_b = a, b
     print(f"Max gap of prime numbers up to {n} is {max_gap} between {max_a} and {max_b}")