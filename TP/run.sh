#!/bin/bash
#SBATCH --nodes=2 # Run on two nodes
#SBATCH --ntasks-per-node=40 # 1 process per core (40 cores per node)
#SBATCH --time=12:00:00 # My job will take at most 12 hours to run, so find and reserve resources for 12 hours
#SBATCH --job-name=Fractal-Generation # Name of my job
#SBATCH --output=Fractal-Generation%j.txt # Write the output to Fractal-Generation<jobID>.txt
#SBATCH --error=Fractal-Generation%j.err # Write stderr to Fractal-Generation<jobID>.err
#SBATCH --mail-user=sekh4498@mylaurier.ca # Email me at this address if my job fails
#SBATCH --mail-type=FAIL

cd $SLURM_SUBMIT_DIR
module restore TP_MODULES

dimension_of_image=1000
processor_count=80

# Complex Numbers to use for the fractal generation
mpirun -np $processor_count python3 ./main.py -- -1 $dimension_of_image
mpirun -np $processor_count python3 ./main.py -- 0.3-0.4j $dimension_of_image
mpirun -np $processor_count python3 ./main.py -- 0.360284+0.100376j $dimension_of_image
mpirun -np $processor_count python3 ./main.py -- -0.1+0.8j $dimension_of_image