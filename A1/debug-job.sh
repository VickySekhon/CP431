#!/bin/bash
#SBATCH --nodes=2 # Run on two nodes
#SBATCH --ntasks-per-node=40 # 1 process per core (40 cores per node)
#SBATCH --time=00:15:00 # My job will take at most 15 minutes to run, so find and reserve resources for 15 minutes
#SBATCH --job-name=prime-gaps # Name of my job
#SBATCH --output=prime-gaps%j.txt # Write the output to prime-gaps<jobID>.txt
#SBATCH --mail-user=sekh4498@mylaurier.ca # Email me at this address if my job fails
#SBATCH --mail-type=FAIL

cd $SLURM_SUBMIT_DIR/A1/output
module restore ASSIGNMENT_1_MODULES

mpirun -np 80 python3 ./prime-gaps.py