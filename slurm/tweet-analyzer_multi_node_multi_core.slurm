#!/bin/bash
#SBATCH --time=00:15:00
#SBATCH --partition=cloud
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
module purge
module load Python/3.5.2-goolf-2015a
mpirun python ../tweetanalyzer/main.py