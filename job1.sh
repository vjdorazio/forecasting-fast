#!/bin/bash
#SBATCH --job-name=grid_forecast
#SBATCH --array=1-12
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=32G
#SBATCH --time=4:00:00
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
##SBATCH --nodelist= dscog002, dscog003
source ~/.bashrc
conda activate r_clean

ulimit -n 100000                         # to avoid “too many open files”
export JAVA_HOME=~/java/jdk-11
export PATH=$JAVA_HOME/bin:$PATH


# Per-task unique port pair (REST + internal)
export H2O_REST_PORT=$((45000 + 2 * SLURM_ARRAY_TASK_ID))

# Safe temp dir for logs and spillover files
export ICE_ROOT="${SLURM_TMPDIR:-/tmp}/h2o_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$ICE_ROOT"

# Forecast date input (1 per array task)
FORECAST_DATE=$(sed -n ${SLURM_ARRAY_TASK_ID}p forecast_dates.txt)

# Run forecast job
Rscript views1.R "$FORECAST_DATE"