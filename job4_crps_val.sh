#!/bin/bash
#SBATCH --job-name=FFP_CRPS_val
#SBATCH --array=1-12                 # Adjust based on the mode (12 for baseline, more for tlags)
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=6
#SBATCH --mem=46G
#SBATCH --time=4:00:00
#SBATCH --output=logs/views4_%x_%A_%a.out
#SBATCH --error=logs/views4_%x_%A_%a.err

source /shared/software/conda/conda_init.sh
source ~/.bashrc


conda activate /scratch/ps00068/r_clean
# ---------------- Config ----------------
HP_TAG="lambda1_twp1.75_mm20_rt9000"    # Match the tag used for draws
DRAWS_DIR="/users/ps00068/scratch/forecasting-fast/data/draws_val/$HP_TAG"

# Mode: baseline or tlags (pass as argument when submitting)
MODE=$1

# --------- SELECT FILES BASED ON MODE ---------
if [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$DRAWS_DIR"/*_tlags_lambda1_*.parquet | sort))
elif [ "$MODE" == "specific" ]; then
    ID_KEY="tlags_tlag1_dr_mod_gs__pred_growseasdummy"
    FILES=($(ls "$DRAWS_DIR"/*"$ID_KEY"*.parquet | sort))
else
    echo "❌ Invalid mode: $MODE"
    exit 1
fi
# ---------------- Pick File for This Task ----------------
DRAW_FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"

echo "Processing CRPS for: $DRAW_FILE"
Rscript views4_crps_val.R "$DRAW_FILE"
echo "Done with: $DRAW_FILE"