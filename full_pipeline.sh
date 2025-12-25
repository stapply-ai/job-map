# To run everything manually:
#   bash /Users/kalilbouzigues/Projects/stapply-ai/data/full_pipeline.sh all

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_BIN="$PROJECT_ROOT/.venv/bin"
PYTHON="$VENV_BIN/python"
LOG_FILE="$PROJECT_ROOT/logs/ai.log"

# Ensure Homebrew binaries are in PATH for cron (vercel CLI is installed via Homebrew)
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

# Detect if running interactively (from terminal, not cron) and initialize LOG_TO_TERMINAL
# Must be done early before any functions that use it
if [[ -t 1 ]]; then
  LOG_TO_TERMINAL=1
else
  LOG_TO_TERMINAL=0
fi

# Error logging function
log_error() {
  local error_msg="[ERROR] $(date '+%Y-%m-%d %H:%M:%S') - $*"
  {
    echo "$error_msg"
    echo "[ERROR] Command: $BASH_COMMAND"
    echo "[ERROR] Exit code: $?"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$error_msg" >&2
  fi
}

# Trap to catch errors and log them
trap 'log_error "Script failed at line $LINENO"' ERR

# Helpful to understand the cron environment when this script runs
log_startup() {
  local startup_msg="=== full_pipeline.sh run at $(date) ==="
  {
    echo
    echo "$startup_msg"
    echo "USER=$USER"
    echo "PATH=$PATH"
    echo "PROJECT_ROOT=$PROJECT_ROOT"
    echo "PYTHON=$PYTHON"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo
    echo "$startup_msg"
    echo "USER=$USER"
    echo "PATH=$PATH"
    echo "PROJECT_ROOT=$PROJECT_ROOT"
    echo "PYTHON=$PYTHON"
  fi
}
log_startup

# Verify virtual environment exists
if [[ ! -f "$PYTHON" ]]; then
  {
    echo "[ERROR] Python interpreter not found at: $PYTHON"
    echo "[ERROR] Virtual environment may not exist at: $VENV_BIN"
    echo "[ERROR] Please ensure .venv is set up correctly"
  } >> "$LOG_FILE" 2>&1
  exit 1
fi

# Verify Python is executable
if [[ ! -x "$PYTHON" ]]; then
  {
    echo "[ERROR] Python interpreter is not executable: $PYTHON"
  } >> "$LOG_FILE" 2>&1
  exit 1
fi

# Helper function to run Python scripts with error handling
run_python_script() {
  local script_path="$1"
  local script_name="$2"
  
  set +e
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    # When interactive, show output in both terminal and log file
    "$PYTHON" "$script_path" 2>&1 | tee -a "$LOG_FILE"
    local exit_code=${PIPESTATUS[0]}
  else
    # When non-interactive (cron), only log to file
    "$PYTHON" "$script_path" >> "$LOG_FILE" 2>&1
    local exit_code=$?
  fi
  set -e
  
  if [[ $exit_code -ne 0 ]]; then
    {
      echo "[ERROR] $script_name failed with exit code $exit_code"
    } >> "$LOG_FILE" 2>&1
    if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
      echo "[ERROR] $script_name failed with exit code $exit_code" >&2
    fi
    return 1
  fi
  return 0
}

run_ashby() {
  local msg="[run_ashby] Starting Ashby pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
  
  if ! run_python_script "$PROJECT_ROOT/ashby/main.py" "run_ashby: main.py"; then
    return 1
  fi
  
  if ! run_python_script "$PROJECT_ROOT/ashby/export_to_csv.py" "run_ashby: export_to_csv.py"; then
    return 1
  fi
  
  msg="[run_ashby] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_greenhouse() {
  local msg="[run_greenhouse] Starting Greenhouse pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
  
  if ! run_python_script "$PROJECT_ROOT/greenhouse/main.py" "run_greenhouse: main.py"; then
    return 1
  fi
  
  if ! run_python_script "$PROJECT_ROOT/greenhouse/export_to_csv.py" "run_greenhouse: export_to_csv.py"; then
    return 1
  fi
  
  msg="[run_greenhouse] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_lever() {
  local msg="[run_lever] Starting Lever pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
  
  if ! run_python_script "$PROJECT_ROOT/lever/main.py" "run_lever: main.py"; then
    return 1
  fi
  
  if ! run_python_script "$PROJECT_ROOT/lever/export_to_csv.py" "run_lever: export_to_csv.py"; then
    return 1
  fi
  
  msg="[run_lever] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_workable() {
  local msg="[run_workable] Starting Workable pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi

  if ! run_python_script "$PROJECT_ROOT/workable/main.py" "run_workable: main.py"; then
    return 1
  fi

  if ! run_python_script "$PROJECT_ROOT/workable/export_to_csv.py" "run_workable: export_to_csv.py"; then
    return 1
  fi

  msg="[run_workable] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_google() {
  local msg="[run_google] Starting Google pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi

  if ! run_python_script "$PROJECT_ROOT/google/main.py" "run_google: main.py"; then
    return 1
  fi

  msg="[run_google] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_amazon() {
  local msg="[run_amazon] Starting Amazon pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi

  if ! run_python_script "$PROJECT_ROOT/amazon/main.py" "run_amazon: main.py"; then
    return 1
  fi

  msg="[run_amazon] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_meta() {
  local msg="[run_meta] Starting Meta pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi

  if ! run_python_script "$PROJECT_ROOT/meta/main.py" "run_meta: main.py"; then
    return 1
  fi

  msg="[run_meta] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_apple() {
  local msg="[run_apple] Starting Apple pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi

  if ! run_python_script "$PROJECT_ROOT/apple/main.py" "run_apple: main.py"; then
    return 1
  fi

  msg="[run_apple] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_workday() {
  "$PYTHON" "$PROJECT_ROOT/workday/main.py"
  "$PYTHON" "$PROJECT_ROOT/workday/export_to_csv.py"
}

run_ai() {
  local msg="[run_ai] Starting AI pipeline at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
  
  if ! run_python_script "$PROJECT_ROOT/ai.py" "run_ai: ai.py"; then
    return 1
  fi
  
  # Resolve vercel in the current PATH so failures are visible in logs
  VERCEL_BIN="$(command -v vercel || true)"
  if [[ -z "$VERCEL_BIN" ]]; then
    msg="[run_ai] vercel CLI not found in PATH, skipping deploy"
    {
      echo "$msg"
    } >> "$LOG_FILE" 2>&1
    if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
      echo "$msg"
    fi
  else
    msg="[run_ai] Deploying to Vercel at $(date '+%Y-%m-%d %H:%M:%S')..."
    {
      echo "$msg"
    } >> "$LOG_FILE" 2>&1
    if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
      echo "$msg"
    fi
    
    set +e
    if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
      (cd "$PROJECT_ROOT/map" && "$VERCEL_BIN" --prod 2>&1 | tee -a "$LOG_FILE")
      local exit_code=${PIPESTATUS[0]}
    else
      (cd "$PROJECT_ROOT/map" && "$VERCEL_BIN" --prod >> "$LOG_FILE" 2>&1)
      local exit_code=$?
    fi
    set -e
    
    if [[ $exit_code -ne 0 ]]; then
      msg="[ERROR] run_ai: Vercel deployment failed with exit code $exit_code"
      {
        echo "$msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$msg" >&2
      fi
      return 1
    fi
  fi
  
  msg="[run_ai] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

run_fetch_job() {
  local msg="[run_fetch_job] Starting fetch_job at $(date '+%Y-%m-%d %H:%M:%S')..."
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
  
  if ! run_python_script "$PROJECT_ROOT/fetch_job.py" "run_fetch_job: fetch_job.py"; then
    return 1
  fi
  
  msg="[run_fetch_job] Completed successfully at $(date '+%Y-%m-%d %H:%M:%S')"
  {
    echo "$msg"
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$msg"
  fi
}

JOB="${1:-all}"

# Track if any step failed
FAILED=0

# Temporarily disable exit on error for function calls so we can handle errors gracefully
set +e

case "$JOB" in
  ashby)
    if ! run_ashby; then
      FAILED=1
    fi
    ;;
  greenhouse)
    if ! run_greenhouse; then
      FAILED=1
    fi
    ;;
  lever)
    if ! run_lever; then
      FAILED=1
    fi
    ;;
  workable)
    if ! run_workable; then
      FAILED=1
    fi
    ;;
  google)
    if ! run_google; then
      FAILED=1
    fi
    ;;
  amazon)
    if ! run_amazon; then
      FAILED=1
    fi
    ;;
  meta)
    if ! run_meta; then
      FAILED=1
    fi
    ;;
  apple)
    if ! run_apple; then
      FAILED=1
    fi
    ;;
  workday)
    run_workday
    ;;
  ai)
    if ! run_ai; then
      FAILED=1
    fi
    ;;
  fetch_job)
    if ! run_fetch_job; then
      FAILED=1
    fi
    ;;
  all)

    msg="[pipeline] Running full pipeline (all jobs)..."
    {
      echo "$msg"
    } >> "$LOG_FILE" 2>&1
    if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
      echo "$msg"
    fi
    
    if ! run_ashby; then
      FAILED=1
      warn_msg="[WARNING] run_ashby failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi
    
    if ! run_greenhouse; then
      FAILED=1
      warn_msg="[WARNING] run_greenhouse failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi
    
    if ! run_lever; then
      FAILED=1
      warn_msg="[WARNING] run_lever failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi
    
    if ! run_workable; then
      FAILED=1
      warn_msg="[WARNING] run_workable failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi

    if ! run_google; then
      FAILED=1
      warn_msg="[WARNING] run_google failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi

    if ! run_amazon; then
      FAILED=1
      warn_msg="[WARNING] run_amazon failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi

    if ! run_meta; then
      FAILED=1
      warn_msg="[WARNING] run_meta failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi

    if ! run_apple; then
      FAILED=1
      warn_msg="[WARNING] run_apple failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi

    if ! run_ai; then
      FAILED=1
      warn_msg="[WARNING] run_ai failed, continuing with other jobs..."
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi

    if ! run_fetch_job; then
      FAILED=1
      warn_msg="[WARNING] run_fetch_job failed"
      {
        echo "$warn_msg"
      } >> "$LOG_FILE" 2>&1
      if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
        echo "$warn_msg" >&2
      fi
    fi
    ;;
  *)
    {
      echo "[ERROR] Invalid job: $JOB"
      echo "[ERROR] Usage: $0 {ashby|greenhouse|lever|workable|google|amazon|meta|apple|ai|fetch_job|all}"
    } >> "$LOG_FILE" 2>&1
    exit 1
    ;;
esac

# Re-enable exit on error
set -e

# Log completion status
if [[ $FAILED -eq 0 ]]; then
  completion_msg="[pipeline] ✅ Pipeline completed successfully at $(date)"
  {
    echo "$completion_msg"
    echo "=== Pipeline run finished ==="
    echo
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$completion_msg"
    echo "=== Pipeline run finished ==="
  fi
else
  completion_msg="[pipeline] ❌ Pipeline completed with errors at $(date)"
  {
    echo "$completion_msg"
    echo "=== Pipeline run finished ==="
    echo
  } >> "$LOG_FILE" 2>&1
  if [[ ${LOG_TO_TERMINAL:-0} -eq 1 ]]; then
    echo "$completion_msg" >&2
    echo "=== Pipeline run finished ===" >&2
  fi
fi

# Exit with appropriate code
if [[ $FAILED -eq 1 ]]; then
  exit 1
fi
