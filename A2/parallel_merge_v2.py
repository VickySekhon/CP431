from mpi4py import MPI
import numpy as np
import sys
import time

FIRST = 0
RANDOM_STEP_SIZE = 10
TAG_SEND_A = 101
TAG_SEND_B = 102
TAG_RECEIVE_C = 103

def send_segments_to_ranks(comm, sendbuf, counts, displacements, recvbuf, root=0, tag=0):
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    count = int(counts[rank])
    if recvbuf.size != count:
        raise ValueError(f"Receive buffer size {recvbuf.size} does not match expected count {count} for rank {rank}")
    
    if rank == root:
        # root copes its own slice
        s = int(displacements[root])
        e = s + int(counts[root])
        recvbuf[:] = sendbuf[s:e]
        
        # root sends everyone else their slice
        for dest in range(size):
            if dest == root:
                continue
            s = int(displacements[dest])
            e = s + int(counts[dest])
            comm.Send([sendbuf[s:e], MPI.INT], dest=dest, tag=tag)
    else:
        comm.Recv([recvbuf, MPI.INT], source=root, tag=tag)
        
def receive_segments_from_ranks(comm, sendbuf, counts, displacements, root=0, tag=0):
    rank = comm.Get_rank()
    size = comm.Get_size()
    
    expected = int(counts[rank])
    if sendbuf.size != expected:
        raise ValueError(f"Send buffer size {sendbuf.size} does not match expected count {expected} for rank {rank}")
    
    if rank == root:
        total = int(np.sum(counts))
        recvbuf = np.empty(total, dtype=np.int32)
        
        # place root's own chunk
        s = int(displacements[root])
        e = s + int(counts[root])
        recvbuf[s:e] - sendbuf
        
        # receive others and place
        for srce in range(size):
            if srce == root:
                continue
            count = int(counts[srce])
            tmp = np.empty(count, dtype=np.int32)
            comm.Recv([tmp, MPI.INT], source=srce, tag=tag)
            s = int(displacements[srce])
            recvbuf[s:s+count] = tmp
        return recvbuf
    else:
        comm.Send([sendbuf, MPI.INT], dest=root, tag=tag)
        return None

def generate_sorted_arrays(n, step = RANDOM_STEP_SIZE):
    # generate A and B sorted arrays of size n
    rnge = np.random.default_rng(int(time.time()))
    A = np.empty(n, dtype=np.int32)
    B = np.empty(n, dtype=np.int32)
    
    A[0] = rnge.integers(0, step, dtype=np.int32)
    B[0] = rnge.integers(0, step, dtype=np.int32)
    
    for i in range(1, n):
        max_increments_A = max(1, i * step - int(A[i-1]))
        A[i] = int(A[i-1]) + int(rnge.integers(0, max_increments_A))
        max_increments_B = max(1, i * step - int(B[i-1]))
        B[i] = int(B[i-1]) + int(rnge.integers(0, max_increments_B))
        
    return A,B

def merge(A, B, out):
    """Sequential merge based on notes from class
    """
    i = 0
    j =0 
    k = 0

    na = A.size
    nb = B.size

    while i < na and j < nb:
        if A[i] < B[j]:
            out[k] = A[i]
            i += 1
        else:
            out[k] = B[j]
            j += 1
        k += 1
    if i < na:
        out[k:k+(na-i)] = A[i:]
    else:
        out[k:k+(nb-j)] = B[j:]
    
    

def parallel_merge_algorithm(exp):
    """
    As specified in notes from class
    n = 2^k, k = log2(n) = exp
    r = n /k groups of A, each of size k
    j(i) is computed using binary search 
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    P = comm.Get_size()
    
    k = exp
    n = 2**k
    
    if n % k != 0:
        if rank == FIRST:
            print("n must be divisible by k")
        return
    
    r = n // k # number of groups
    
    # group to rank mapping --> distribute the groups across ranks where extra groups go to higher ranks
    base = r // P
    rem = r % P
    groups_per_rank = np.full(P, base, dtype=np.int32)
    if rem > 0:
        groups_per_rank[P - rem:] +=1 
        
    group_displacements = np.zeros(P, dtype=np.int32)
    if P > 1:
        group_displacements[1:] = np.cumsum(groups_per_rank[:-1], dtype=np.int32)
    
    
    # rank-level A counts and displacements
    a_counts = (groups_per_rank * k).astype(np.int32)
    a_displacements = (group_displacements * k).astype(np.int32)
    
    # rank-level B counts and displacements
    b_counts = np.empty(P, dtype=np.int32)
    b_displacements = np.empty(P, dtype=np.int32)
    
    # broadcast group-level B boundaries so each rank can merge in groups
    end_exclusive = np.empty(r, dtype=np.int32)
    
    if rank == FIRST:
        A, B = generate_sorted_arrays(n)
        
        # keys are the last eleemnts of each A group: A[k-1], A[2k-1], ..., A[rk-1]
        keys = A[k-1::k] # length r
        
        # B < key => end_exclusive = lower_bound(B, key)
        end_exclusive = np.searchsorted(B, keys, side='left').astype(np.int32)
        
        # last group will take the remainder of B
        end_exclusive[-1] = n
        
        start_exclusive = np.empty(r, dtype=np.int32)
        start_exclusive[0] = 0
        start_exclusive[1:] = end_exclusive[:-1]
        
        
        # compute rank-level B displacements/counts by combining consecutive groups for that rank
        for process in range(P):
            group_start = int(group_displacements[process])
            group_count = int(groups_per_rank[process])
            
            if group_count == 0:
                b_displacements[process] = 0
                b_counts[process] = 0
                continue
            
            group_end = group_start + group_count - 1
            b_start = int(start_exclusive[group_start])
            b_end = int(end_exclusive[group_end])
            
            b_displacements[process] = b_start
            b_counts[process] = b_end - b_start
    
    else: 
        A = None
        B = None
        
    # broadcast metadata + group-level boundaries to all ranks so they can merge in groups
    comm.Bcast(a_counts, root=FIRST)
    comm.Bcast(a_displacements, root=FIRST)
    comm.Bcast(b_counts, root=FIRST)
    comm.Bcast(b_displacements, root=FIRST)
    comm.Bcast(end_exclusive, root=FIRST)
    
    # local buffers
    local_A = np.empty(int(a_counts[rank]), dtype=np.int32)
    local_B = np.empty(int(b_counts[rank]), dtype=np.int32)
    
    comm.Barrier()
    t0 = MPI.Wtime()
    
    # Scatter A and B
    comm.Scatterv([A, a_counts, a_displacements, MPI.INT] if rank == FIRST else None, local_A, root=FIRST)
    comm.Scatterv([B, b_counts, b_displacements, MPI.INT] if rank == FIRST else None, local_B, root=FIRST)
    
    # merge group by group
    group_start = int(group_displacements[rank])
    group_count = int(groups_per_rank[rank])
    
    # merge local_A and local_b into local_c
    def b_group_start(group):
        return 0 if group == 0 else int(end_exclusive[group-1])
    
    # pre allocate local_C total size equal to local_A + local_B
    local_C = np.empty(local_A.size + local_B.size, dtype=np.int32)
    
    # offsets into local_A/local_B
    a_offset = 0
    c_offset = 0
    
    
    # local_b corresponds to the global B slice
    b_base = int(b_displacements[rank])
    
    for group in range(group_start, group_start+group_count):
        # A subgroup - fixed size k
        A_group = local_A[a_offset:a_offset+k]
        a_offset += k
        
        # B subgroup 
        gb_start = b_group_start(group)
        gb_end = int(end_exclusive[group])
        
        lb_start = gb_start-b_base
        lb_end = gb_end-b_base
        if lb_start < 0:
            lb_start = 0
        if lb_end < lb_start:
            lb_end = lb_start
            
        B_group = local_B[lb_start:lb_end]
        
        out_len = A_group.size + B_group.size
        merge(A_group, B_group, local_C[c_offset:c_offset+out_len])
        c_offset += out_len
        
    # gather results into C
    c_counts = (a_counts + b_counts).astype(np.int32)
    c_displacements = np.zeros(P, dtype=np.int32)
    if P > 1: 
        c_displacements[1:] = np.cumsum(c_counts[:-1], dtype = np.int64).astype(np.int32)
        
    if rank == FIRST:
        C = np.empty(2*n, dtype=np.int32)
    else:
        C = None
        
    comm.Gatherv(local_C, [C, c_counts, c_displacements, MPI.INT] if rank == FIRST else None, root=FIRST)
    t1 = MPI.Wtime()
    
    if rank == FIRST:
        print("Time taken: {}".format(t1-t0))
        print("Check if C is sorted: {}".format(is_sorted(C)))
        print(C)

def is_sorted(array):
    return bool(np.all(array[:-1] <= array[1:]))
              
if __name__ == "__main__":
    if len(sys.argv) < 2:
        if MPI.COMM_WORLD.Get_rank() == FIRST:
            print("Usage: mpirun -np <num_processes> python parallel_merge_v2.py <exp>")
        sys.exit(0)
    
    parallel_merge_algorithm(int(sys.argv[1]))