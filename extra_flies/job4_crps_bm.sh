#!/bin/bash
#SBATCH --job-name=FFP_CRPS_BENCH
#SBATCH --array=1-12
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=2
#SBATCH --mem=16G
#SBATCH --time=4:00:00
#SBATCH --output=logs/bench_crps_%x_%A_%a.out
#SBATCH --error=logs/bench_crps_%x_%A_%a.err

# Load environment
source /shared/software/conda/conda_init.sh
source ~/.bashrc
conda activate /scratch/ps00068/r_clean

# -------- Config --------
BENCHMARK_NAME=$1
DRAWS_DIR="$HOME/scratch/forecasting-fast/data/benchmark/draws/$BENCHMARK_NAME"

# List draw files
FILES=($(ls "$DRAWS_DIR"/*.parquet))
DRAW_FILE="${FILES[$SLURM_ARRAY_TASK_ID-1]}"

echo "Processing CRPS for: $DRAW_FILE"
Rscript views4_crps_bm.R "$DRAW_FILE"
echo "Done with: $DRAW_FILE"
