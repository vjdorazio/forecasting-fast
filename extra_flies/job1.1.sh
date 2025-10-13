#!/bin/bash

#SBATCH --job-name=grid_forecast_cum
#SBATCH -N 1
#SBATCH -c 6
#SBATCH -n 1
#SBATCH --array=1-3
#SBATCH -p standby
#SBATCH -t 04:00:00
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=32G

source /shared/software/conda/conda_init.sh
source ~/.bashrc
conda activate /scratch/ps00068/r_clean

ulimit -n 100000
export JAVA_HOME=/scratch/ps00068/r_clean
export PATH=$JAVA_HOME/bin:$PATH

# Hyperparams - keep as-is
declare -a HYPERPARAMS_COMBOS=("1,1.75,20")

# Horizons to run
declare -a HORIZONS=(3 6 12)

TASK_ID=$((SLURM_ARRAY_TASK_ID - 1))
HYPERPARAM_IDX=0
HORIZON_IDX=$(( TASK_ID % ${#HORIZONS[@]} ))

line="${HYPERPARAMS_COMBOS[$HYPERPARAM_IDX]}"
IFS=',' read -r lambda tweedie_power mm <<< "$line"

export LAMBDA="$lambda"
export TWEEDIE_POWER="$tweedie_power"
export MM="$mm"
export HORIZON="${HORIZONS[$HORIZON_IDX]}"

# Per-task unique ports
export H2O_REST_PORT=$((45000 + 2 * SLURM_ARRAY_TASK_ID))
export ICE_ROOT="${SLURM_TMPDIR:-/tmp}/h2o_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$ICE_ROOT"

#  - views1.R will use its default Oct 2023
Rscript views1.1.R