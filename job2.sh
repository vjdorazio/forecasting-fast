#!/bin/bash
#SBATCH --job-name=FFP
#SBATCH --array=1-12
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --mem=48G
#SBATCH --time=4:00:00
#SBATCH --output=logs/views2_%x_%A_%a.out
#SBATCH --error=logs/views2_%x_%A_%a.err
  # Or your R environment
source /shared/software/conda/conda_init.sh
source ~/.bashrc


conda activate /scratch/ps00068/r_clean
# Base directory (same hp_tag as views1 runs)
HP_TAG="lambda1_twp1.75_mm20_rt9000"
BASE_DIR="/users/ps00068/scratch/forecasting-fast/data/predictions/$HP_TAG"

# Choose mode: "baseline" or "tlags" (pass this when submitting)
MODE=$1

# --------- SELECT FILES BASED ON MODE ---------
if [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$BASE_DIR"/*_tlags_lambda1_*.parquet | sort))
elif [ "$MODE" == "specific" ]; then
    ID_KEY="tlags_tlag1_dr_mod_gs__pred_growseasdummy"
    FILES=($(ls "$BASE_DIR"/*"$ID_KEY"*.parquet | sort))
else
    echo "❌ Invalid mode: $MODE"
    exit 1
fi

# Pick the file for this array task
INPUT_FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"

echo "Processing: $INPUT_FILE"

# Run the views2 script
Rscript views2.R "$INPUT_FILE"