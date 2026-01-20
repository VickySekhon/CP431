import math
# from mpi4py import MPI

# comm = MPI.COMM_WORLD

# my_rank = comm.Get_rank()
# p = comm.Get_size()


# determine if a number is prime
def is_prime(n):     
     if n < 2: return False
     
     upper_bound = math.floor(math.sqrt(n)) + 1 # account for "range" 
     for i in range(2, upper_bound):
          if n % i == 0:
               return False
     return True

# find prime numbers up to a constant 'limit'
def find_primes(limit):
     primes = []
     for i in range(1,limit+1):
          if is_prime(i): primes.append(i)
     return primes


def compute_max_distance(LIMIT):     
     primes = find_primes(LIMIT)
     n = len(primes)
     
     # keep track of a max distance variable
     max_distance = 0
     a, b = None, None
     
     # iterate through prime numbers
     for i, curr in enumerate(primes):
          if i + 1 < n:
               next_ = primes[i+1]
               if next_ - curr > max_distance:
                    max_distance = next_ - curr
                    a, b = curr, next_
     
     return max_distance, a, b

LIMIT = 10000000
dist, a, b = compute_max_distance(LIMIT)
print(f"Max distance of prime numbers up to {LIMIT} is {dist}, between numbers {a} and {b}.")
               

          
     
     