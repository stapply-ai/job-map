#!/bin/bash
set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions (all output to stderr to avoid interfering with command substitution)
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" >&2
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" >&2
}

info() {
    echo -e "${BLUE}[INFO]${NC} $1" >&2
}

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    error "Virtual environment not found. Please create it first."
    exit 1
fi

# Activate virtual environment
source .venv/bin/activate

# Create logs directory
mkdir -p logs
TIMESTAMP=$(date +'%Y%m%d_%H%M%S')
LOG_DIR="logs/${TIMESTAMP}"
mkdir -p "$LOG_DIR"

log "Starting pipeline run (timestamp: $TIMESTAMP)"
log "Logs will be saved to: $LOG_DIR"

# Array to track PIDs and their corresponding platforms
declare -a PIDS=()
declare -a PID_PLATFORMS=()
declare -a PLATFORMS=("ashby" "greenhouse" "lever" "workable")

# Function to run a script and log output
# Returns only the PID (to stdout), logs to stderr
run_with_logging() {
    local platform=$1
    local script=$2
    local log_file="$LOG_DIR/${platform}_${script}.log"
    
    log "Starting ${platform}/${script}"
    python "${platform}/${script}" > "$log_file" 2>&1 &
    local pid=$!
    echo $pid  # Only output PID to stdout
}

# Phase 1: Run all main.py scripts in parallel
log "═══════════════════════════════════════════════════════════"
log "Phase 1: Running all main.py scripts in parallel..."
log "═══════════════════════════════════════════════════════════"

for platform in "${PLATFORMS[@]}"; do
    if [ ! -f "${platform}/main.py" ]; then
        error "${platform}/main.py not found, skipping..."
        continue
    fi
    pid=$(run_with_logging "$platform" "main.py")
    PIDS+=($pid)
    PID_PLATFORMS+=($platform)
    log "Started ${platform}/main.py (PID: $pid)"
done

if [ ${#PIDS[@]} -eq 0 ]; then
    error "No valid main.py scripts found to run"
    exit 1
fi

# Wait for all main.py scripts to complete
log "Waiting for all main.py scripts to complete..."
MAIN_FAILED=0
for i in "${!PIDS[@]}"; do
    pid=${PIDS[$i]}
    platform=${PID_PLATFORMS[$i]}
    if wait $pid; then
        log "✓ ${platform}/main.py completed successfully"
    else
        error "✗ ${platform}/main.py failed (exit code: $?)"
        error "  Check log: $LOG_DIR/${platform}_main.py.log"
        MAIN_FAILED=1
    fi
done

# Check if any main.py failed
if [ $MAIN_FAILED -eq 1 ]; then
    warn "One or more main.py scripts failed. Continuing with exports anyway..."
fi

# Phase 2: Run all export_to_csv.py scripts in parallel
log ""
log "═══════════════════════════════════════════════════════════"
log "Phase 2: Running all export_to_csv.py scripts in parallel..."
log "═══════════════════════════════════════════════════════════"

PIDS=()
PID_PLATFORMS=()
for platform in "${PLATFORMS[@]}"; do
    if [ ! -f "${platform}/export_to_csv.py" ]; then
        error "${platform}/export_to_csv.py not found, skipping..."
        continue
    fi
    pid=$(run_with_logging "$platform" "export_to_csv.py")
    PIDS+=($pid)
    PID_PLATFORMS+=($platform)
    log "Started ${platform}/export_to_csv.py (PID: $pid)"
done

if [ ${#PIDS[@]} -eq 0 ]; then
    error "No valid export_to_csv.py scripts found to run"
    exit 1
fi

# Wait for all export scripts to complete
log "Waiting for all export scripts to complete..."
EXPORT_FAILED=0
for i in "${!PIDS[@]}"; do
    pid=${PIDS[$i]}
    platform=${PID_PLATFORMS[$i]}
    if wait $pid; then
        log "✓ ${platform}/export_to_csv.py completed successfully"
    else
        error "✗ ${platform}/export_to_csv.py failed (exit code: $?)"
        error "  Check log: $LOG_DIR/${platform}_export_to_csv.py.log"
        EXPORT_FAILED=1
    fi
done

# Phase 3: Gather all jobs (optional, only if exports succeeded)
log ""
log "═══════════════════════════════════════════════════════════"
log "Phase 3: Gathering all jobs into consolidated jobs.csv..."
log "═══════════════════════════════════════════════════════════"

if [ $EXPORT_FAILED -eq 0 ] && [ -f "gather_jobs.py" ]; then
    log "Running gather_jobs.py..."
    python gather_jobs.py > "$LOG_DIR/gather_jobs.log" 2>&1
    if [ $? -eq 0 ]; then
        log "✓ Successfully gathered all jobs"
    else
        error "✗ gather_jobs.py failed"
        error "  Check log: $LOG_DIR/gather_jobs.log"
    fi
elif [ $EXPORT_FAILED -eq 1 ]; then
    warn "Skipping gather_jobs.py due to export failures"
elif [ ! -f "gather_jobs.py" ]; then
    warn "gather_jobs.py not found, skipping consolidation"
fi

# Final status
log ""
log "═══════════════════════════════════════════════════════════"
if [ $MAIN_FAILED -eq 0 ] && [ $EXPORT_FAILED -eq 0 ]; then
    log "Pipeline completed successfully! 🎉"
    log "Logs available in: $LOG_DIR"
    exit 0
else
    error "Pipeline completed with errors."
    error "Check logs in: $LOG_DIR"
    exit 1
fi
