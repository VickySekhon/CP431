#!/bin/bash
#SBATCH --nodes=2
#SBATCH --cpus-per-task=40
#SBATCH --time=00:15:00
#SBATCH --job-name debug-job
#SBATCH --output=debug-job-out%j.txt
#SBATCH --mail-type=FAIL
 

cd $SLURM_SUBMIT_DIR/A1
module restore ASSIGNMENT_1_MODULES

mpirun -np 80 python3 ./prime-gaps.py