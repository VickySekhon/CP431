from mpi4py import MPI
import numpy as np


# Implementation of MPI- Scatter
# size of array to be sorted
n = 100000
def parallel_merge_sort(n):
    # set up MPI
    comm = MPI.COMM_WORLD
    size = comm.Get_size()
    rank = comm.Get_rank() 

    # size of each subarray
    sub_size = int(n / size)

    # allocate local memory on each rank
    local = np.zeros(sub_size, dtype='int')

    if rank is 0:
        # generate unsorted array
        unsorted = np.random.randint(n, size=n)
        
    comm.Scatter(unsorted, local, root=0)

    # MPI merging

    split = size / 2

    while split >=1:
        # rank is in upper half of processes
        # this process is a right child
        if split <= rank < split * 2:
            # send local array to parent to be merged
            comm.Send(local, rank-split, tag=0)
            
        # rank is in lower half of processes
        # this process is a left child/parent
        elif rank < split:
            # allocate memory for right child's array
            tmp = np.zeros(local.size, dtype='int')
            # allocate memory for the merged result
            result = np.zeros(2*local.size, dtype='int')
            # receive data from right child
            comm.Recv(tmp, rank+split, tag=0)
            
            # merge arrays
            result = merge(local, tmp)
            local = np.array(result)
            
        # update split as we have removed # bottom
        # of process tree or half of the nodes
        split = split / 2
    
    
def merge(left, right):
    # merge two sorted 1-D numpy arrays to return a new single sorted array
    i = 0
    j = 0
    out = np.empty(left.size + right.size, dtype=left.dtype)
    k = 0
    while i < left.size and j < right.size:
        if left[i] <= right[j]:
            out[k] = left[i]
            i += 1
        else: 
            out[k] = right[j]
            j += 1
        k += 1
    
    if i < left.size:
        out[k:] = left[i:]
    elif j < right.size:
        out[k:] = right[j:]
    
    return out

def compute_counts_and_displacement(total_n, size):
    counts = [total_n // size + (1 if i < (total_n % size) else 0) for i in range(size)]
    displacement = [0] * size
    for i in range(1, size):
        displacement[i] = displacement[i-1] + counts[i-1]
    return np.array(counts, dtype=int), np.array(displacement, dtype=int)
    
