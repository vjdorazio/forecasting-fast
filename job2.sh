#!/bin/bash
#SBATCH --job-name=FFP
#SBATCH --array=1-12
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=48:00:00
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

# Dynamically build the file list based on the mode
if [ "$MODE" == "baseline" ]; then
    FILES=($(ls "$BASE_DIR"/*.parquet | grep -v "tlags" | grep -v "splags" | grep -v "mov_sum"))
elif [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$BASE_DIR"/*_tlags_*.parquet | grep -v "splags" | grep -v "mov_sum"))
else
    echo "Invalid mode. Use 'baseline' or 'tlags'."
    exit 1
fi

# Pick the file for this array task
INPUT_FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"

echo "Processing: $INPUT_FILE"

# Run the views2 script
Rscript views2.R "$INPUT_FILE"