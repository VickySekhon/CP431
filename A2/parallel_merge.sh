#!/bin/bash
#SBATCH --nodes=2 # Run on two nodes
#SBATCH --ntasks-per-node=40 # 1 process per core (40 cores per node)
#SBATCH --time=05:00:00 # My job will take at most 5 hours to run, so find and reserve resources for 5 hours
#SBATCH --job-name=parallel-merge # Name of my job
#SBATCH --output=parallel-merge%j.txt # Write the output to parallel-merge<jobID>.txt
#SBATCH --mail-user=sekh4498@mylaurier.ca # Email me at this address if my job fails
#SBATCH --mail-type=FAIL

cd $SLURM_SUBMIT_DIR/A1/output
module restore ASSIGNMENT_2_MODULES

mpirun -np 80 python3 ./parallel_merge_v3.py 40