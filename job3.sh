#!/bin/bash
#SBATCH --job-name=FFP_Draws
#SBATCH --array=1-12                # 12 files per run
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=48G
#SBATCH --time=4:00:00
#SBATCH --output=logs/views3_%x_%A_%a.out
#SBATCH --error=logs/views3_%x_%A_%a.err

source /shared/software/conda/conda_init.sh
source ~/.bashrc

conda activate /scratch/ps00068/r_clean


# Set the hyperparameter tag (match the forecasts folder)
HP_TAG="lambda1_twp1.75_mm20_rt9000"
FORECAST_DIR="/users/ps00068/scratch/forecasting-fast/data/predictions/$HP_TAG"

# Mode must be passed when submitting (either "baseline" or "tlags")
MODE=$1
if [ -z "$MODE" ]; then
    echo "Error: Must specify mode (baseline or tlags)"
    exit 1
fi

# --------- SELECT FILES BASED ON MODE ---------
if [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$FORECAST_DIR"/*_tlags_lambda1_*.parquet | sort))
elif [ "$MODE" == "specific" ]; then
    ID_KEY="tlags_tlag1_dr_mod_gs__pred_growseasdummy"
    FILES=($(ls "$FORECAST_DIR"/*"$ID_KEY"*.parquet | sort))
else
    echo "invalid mode: $MODE"
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
Rscript views3_val.R "$FORECAST_FILE"
echo "Done with: $FORECAST_FILE"
