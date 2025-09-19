#!/bin/bash


#SBATCH --job-name=grid_forecast
#SBATCH -N 1
#SBATCH -c 6
#SBATCH -n 1
#SBATCH --array=1-3
#SBATCH -p standby
#SBATCH -t 04:00:00
#SBATCH --output=logs/%x_%A_%a.out
#SBATCH --error=logs/%x_%A_%a.err
##SBATCH --array=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=48G



source /shared/software/conda/conda_init.sh
source ~/.bashrc


conda activate /scratch/ps00068/r_clean

ulimit -n 100000                         # avoid “too many open files”
export JAVA_HOME=/scratch/ps00068/r_clean
export PATH=$JAVA_HOME/bin:$PATH

# Define hyperparameter combinations directly in the script
declare -a HYPERPARAMS_COMBOS=( 
    "1,1.75,20"
)

# Forecast dates and hyperparams
TOTAL_FORECASTS=$(wc -l < forecast_dates.txt)  
TOTAL_HYPERPARAMS=${#HYPERPARAMS_COMBOS[@]} # Get count from the array

TASK_ID=$((SLURM_ARRAY_TASK_ID - 1))
FORECAST_IDX=$(( (TASK_ID / TOTAL_HYPERPARAMS) + 1 ))  
HYPERPARAM_IDX=$(( TASK_ID % TOTAL_HYPERPARAMS )) # 0-based index for array access

##export WANDB_MODE=offline
##export WANDB_API_KEY=d7d4442d07cdb571d866c54ed0a9a8861bcfae78

# Forecast date input (1 per array task)
FORECAST_DATE=$(sed -n ${FORECAST_IDX}p forecast_dates.txt)
# Load hyperparameter combo from the defined array
line="${HYPERPARAMS_COMBOS[$HYPERPARAM_IDX]}"
IFS=',' read -r lambda tweedie_power mm <<< "$line" 

# Per-task unique port pair (REST + internal)
export H2O_REST_PORT=$((45000 + 2 * SLURM_ARRAY_TASK_ID))

# Safe temp dir for logs and spillover files
export ICE_ROOT="${SLURM_TMPDIR:-/tmp}/h2o_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$ICE_ROOT"

# Pass variables to R
export FORECAST_DATE
export LAMBDA="$lambda"
export TWEEDIE_POWER="$tweedie_power"
export MM="$mm"

# Run forecast job
Rscript views1.R "$FORECAST_DATE"