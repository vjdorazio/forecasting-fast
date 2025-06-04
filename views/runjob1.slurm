#!/bin/bash

#SBATCH -J viewsrun
#SBATCH -N 1
#SBATCH -c 6
#SBATCH -n 1
#SBATCH -p standby
#SBATCH -t 04:00:00

source /shared/software/conda/conda_init.sh
module purge
module load conda
conda activate Rfeb0624

cd /scratch/vjd00006/forecasting
./views1_dub_hpc2024.R $1 $2