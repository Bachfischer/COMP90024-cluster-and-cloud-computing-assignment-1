#!/bin/bash
#SBATCH --time=00:15:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --cpus-per-task 1
#SBATCH --output=C-%x.%j.out
#SBATCH --error=C-%x.%j.err
module purge
module load Python/3.5.2-goolf-2015a
time mpirun python ../main.py --dataset bigTwitter.json --language-codes language_codes.json --batch-size 1024