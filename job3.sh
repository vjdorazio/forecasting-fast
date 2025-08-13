#!/bin/bash
#SBATCH --job-name=FFP_Draws
#SBATCH --array=1-12                # 12 files per run
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output=logs/views3_%x_%A_%a.out
#SBATCH --error=logs/views3_%x_%A_%a.err

source /shared/software/conda/conda_init.sh
source ~/.bashrc


conda activate /scratch/ps00068/r_clean


# Set the hyperparameter tag (match the forecasts folder)
HP_TAG="lambda1_twp1.75_mm20_rt9000"
FORECAST_DIR="/users/ps00068/scratch/forecasting-fast/data/forecasts/$HP_TAG"

# Mode must be passed when submitting (either "baseline" or "tlags")
MODE=$1
if [ -z "$MODE" ]; then
    echo "Error: Must specify mode (baseline or tlags)"
    exit 1
fi

# Filter the forecast files based on mode
if [ "$MODE" == "baseline" ]; then
    FILES=($(ls "$FORECAST_DIR"/*.parquet | grep -v "tlags" | grep -v "splags" | grep -v "mov_sum"))
elif [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$FORECAST_DIR"/*_tlags_*.parquet | grep -v "splags" | grep -v "mov_sum"))
else
    echo "Invalid mode: $MODE. Use 'baseline' or 'tlags'."
    exit 1
fi

# Make sure we actually have 12 files to process
if [ ${#FILES[@]} -lt $SLURM_ARRAY_TASK_MAX ]; then
    echo "Error: Fewer than 12 forecast files found for mode $MODE"
    exit 1
fi

# Pick the forecast file for this task
FORECAST_FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"

echo "Starting draws for: $FORECAST_FILE (mode: $MODE)"
Rscript views3.R "$FORECAST_FILE"
echo "Done with: $FORECAST_FILE"
