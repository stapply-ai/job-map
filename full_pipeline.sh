# To run everything manually:
#   bash /Users/kalilbouzigues/Projects/stapply-ai/data/full_pipeline.sh all

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_BIN="$PROJECT_ROOT/.venv/bin"
PYTHON="$VENV_BIN/python"

run_ashby() {
  "$PYTHON" "$PROJECT_ROOT/ashby/main.py"
  "$PYTHON" "$PROJECT_ROOT/ashby/export_to_csv.py"
}

run_greenhouse() {
  "$PYTHON" "$PROJECT_ROOT/greenhouse/main.py"
  "$PYTHON" "$PROJECT_ROOT/greenhouse/export_to_csv.py"
}

run_lever() {
  "$PYTHON" "$PROJECT_ROOT/lever/main.py"
  "$PYTHON" "$PROJECT_ROOT/lever/export_to_csv.py"
}

run_workable() {
  "$PYTHON" "$PROJECT_ROOT/workable/main.py"
  "$PYTHON" "$PROJECT_ROOT/workable/export_to_csv.py"
}

run_google() {
  "$PYTHON" "$PROJECT_ROOT/google/main.py"
  "$PYTHON" "$PROJECT_ROOT/google/export_to_csv.py"
}

run_ai() {
  "$PYTHON" "$PROJECT_ROOT/ai.py"
  cd "$PROJECT_ROOT/map"
  # Deploy the map app – assumes `vercel` is on PATH for the cron user
  vercel --prod
  cd "$PROJECT_ROOT"
  "$PYTHON" "$PROJECT_ROOT/post.py"
  "$PYTHON" "$PROJECT_ROOT/publish.py"
}

JOB="${1:-all}"

case "$JOB" in
  ashby)
    run_ashby
    ;;
  greenhouse)
    run_greenhouse
    ;;
  lever)
    run_lever
    ;;
  workable)
    run_workable
    ;;
  google)
    run_google
    ;;
  ai)
    run_ai
    ;;
  all)
    run_ashby
    run_greenhouse
    run_lever
    run_workable
    run_google
    run_ai
    ;;
  *)
    echo "Usage: $0 {ashby|greenhouse|lever|workable|google|ai|all}"
    exit 1
    ;;
esac
