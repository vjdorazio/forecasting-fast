#!/bin/bash
#SBATCH --job-name=FFP_CRPS
#SBATCH --array=1-12                 # Adjust based on the mode (12 for baseline, more for tlags)
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output=logs/views4_%x_%A_%a.out
#SBATCH --error=logs/views4_%x_%A_%a.err

source /shared/software/conda/conda_init.sh
source ~/.bashrc


conda activate /scratch/ps00068/r_clean
# ---------------- Config ----------------
HP_TAG="lambda1_twp1.75_mm20_rt9000"    # Match the tag used for draws
DRAWS_DIR="/users/ps00068/scratch/forecasting-fast/data/draws/$HP_TAG"

# Mode: baseline or tlags (pass as argument when submitting)
MODE=$1

# ---------------- Select Files ----------------
if [ "$MODE" == "baseline" ]; then
    FILES=($(ls "$DRAWS_DIR"/*.parquet | grep -v "tlags" | grep -v "splags" | grep -v "mov_sum"))
elif [ "$MODE" == "tlags" ]; then
    FILES=($(ls "$DRAWS_DIR"/*_tlags_*.parquet | grep -v "splags" | grep -v "mov_sum"))
else
    echo "Invalid mode: $MODE. Use 'baseline' or 'tlags'."
    exit 1
fi

# ---------------- Pick File for This Task ----------------
DRAW_FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"

echo "Processing CRPS for: $DRAW_FILE"
Rscript views4.R "$DRAW_FILE"
echo "Done with: $DRAW_FILE"