#!/bin/bash
#SBATCH --job-name=VAL_RMSE
#SBATCH --array=1-12
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=8G
#SBATCH --time=00:30:00
#SBATCH --output=logs/val_rmse_%x_%A_%a.out
#SBATCH --error=logs/val_rmse_%x_%A_%a.err

source /shared/software/conda/conda_init.sh
source ~/.bashrc
conda activate /scratch/ps00068/r_clean

# --------- CONFIG ---------
HP_TAG="lambda1_twp1.75_mm20_rt9000"
PRED_DIR="/users/ps00068/scratch/forecasting-fast/data/predictions/$HP_TAG"

MODE=$1  # "tlags" or "specific"

# --------- SELECT FILES BASED ON MODE ---------
if [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$PRED_DIR"/*_tlags_lambda1_*.parquet | sort))
elif [ "$MODE" == "specific" ]; then
    ID_KEY="tlags_tlag1_dr_mod_gs__pred_growseasdummy"
    FILES=($(ls "$PRED_DIR"/*"$ID_KEY"*.parquet | sort))
else
    echo "❌ Invalid mode: $MODE"
    exit 1
fi

# --------- PICK FILE FOR THIS TASK ----------
FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"
echo "🔍 Task $SLURM_ARRAY_TASK_ID processing: $FILE"

Rscript recalc_val_rmse.R "$FILE" "$MODE"
echo "✅ Finished: $FILE"



