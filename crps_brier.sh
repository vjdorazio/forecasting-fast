#!/bin/bash
#SBATCH --job-name=crps_brier_score
#SBATCH --array=1-12
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=03:00:00
#SBATCH --output=logs/views3_%x_%A_%a.out
#SBATCH --error=logs/views3_%x_%A_%a.err

source /shared/software/conda/conda_init.sh
source ~/.bashrc
conda activate /scratch/ps00068/r_clean

# --------- CONFIG ---------
HP_TAG="lambda1_twp1.75_mm20_rt9000"
PRED_DIR="/users/ps00068/scratch/forecasting-fast/data/2025_forecast/forecast"
MODE=$1        # e.g. "specific" or "tlags"
RUN_FLAG=$2    # "val" or "test"

# --------- SELECT FILES BASED ON MODE ---------
if [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$PRED_DIR"/*_tlags_basline_lambda1_*.parquet | sort))
elif [ "$MODE" == "specific" ]; then
    ID_KEY="tlags_basline_dr_mod_2022growseasdummy"
    FILES=($(ls "$PRED_DIR"/*"$ID_KEY"*.parquet | sort))
else
    echo "Invalid mode: $MODE"
    exit 1
fi

# --------- PICK FILE FOR THIS TASK ----------
FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"
echo "🔍 Task $SLURM_ARRAY_TASK_ID processing: $FILE"

# --------- Run Views R Script ----------
Rscript params_join.R "$RUN_FLAG" "$FILE" "$MODE"  #crps_brier.R  original  other are params_join.R
echo "inished: $FILE"