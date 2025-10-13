#!/bin/bash
#SBATCH --job-name=LOAD_MODEL
#SBATCH --array=1-12
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=12
#SBATCH --mem=48G
#SBATCH --time=01:30:00
#SBATCH --output=logs/load_model_%x_%A_%a.out
#SBATCH --error=logs/load_model_%x_%A_%a.err

source /shared/software/conda/conda_init.sh
source ~/.bashrc
conda activate /scratch/ps00068/r_clean


ulimit -n 100000                         # avoid “too many open files”
export JAVA_HOME=/scratch/ps00068/r_clean
export PATH=$JAVA_HOME/bin:$PATH


# Per-task unique port pair (REST + internal)
export H2O_REST_PORT=$((45000 + 2 * SLURM_ARRAY_TASK_ID))

# Safe temp dir for logs and spillover files
export ICE_ROOT="${SLURM_TMPDIR:-/tmp}/h2o_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$ICE_ROOT"

# ------------ CONFIG -------------
HP_TAG="lambda1_twp1.75_mm20_rt9000"
MODEL_BASE_DIR="/users/ps00068/scratch/forecasting-fast/models/"

MODE=$1   # "tlags" or "specific"

# ------------ SELECT MODEL FOLDERS -------------
if [ "$MODE" == "tlags" ]; then
    MODEL_FOLDERS=($(find "$MODEL_BASE_DIR" -mindepth 1 -maxdepth 1 -type d -name "*_tlags_basline_ged_sb_splag_1_1_sb_1_*" | sort))
elif [ "$MODE" == "specific" ]; then
    ID_KEY="tlags_basline_dr_mod_2022growseasdummy_"
    MODEL_FOLDERS=($(find "$MODEL_BASE_DIR" -mindepth 1 -maxdepth 1 -type d -name "*$ID_KEY*" | sort))
else
    echo "Invalid mode: $MODE"
    exit 1
fi

TASK_INDEX=$((SLURM_ARRAY_TASK_ID - 1))
MODEL_FOLDER="${MODEL_FOLDERS[$TASK_INDEX]}"

echo "Task $SLURM_ARRAY_TASK_ID loading model from: $MODEL_FOLDER"
Rscript  real_prediction.R "$MODEL_FOLDER"  ##recalc_val_rmse.R  #for real prediction real_prediction.R
echo "Fnished: $MODEL_FOLDER"