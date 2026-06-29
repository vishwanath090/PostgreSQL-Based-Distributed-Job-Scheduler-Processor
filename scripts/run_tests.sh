#!/usr/bin/env bash
# scripts/run_tests.sh
# ---------------------
# Pytest runner with named modes. Run from the project root.
#
# Usage:
#   bash scripts/run_tests.sh             # run everything
#   bash scripts/run_tests.sh all         # same as above
#   bash scripts/run_tests.sh api         # API endpoint tests only
#   bash scripts/run_tests.sh once        # exactly-once delivery test
#   bash scripts/run_tests.sh priority    # priority ordering test
#   bash scripts/run_tests.sh retry       # exponential backoff test
#   bash scripts/run_tests.sh dlq         # dead-letter queue test
#   bash scripts/run_tests.sh reaper      # stale-reaper test
#   bash scripts/run_tests.sh fast        # all tests, stop on first failure
#   bash scripts/run_tests.sh verbose     # all tests, full log output
#   bash scripts/run_tests.sh coverage    # all tests + HTML coverage report

set -euo pipefail

# ── colours ──────────────────────────────────────────────────────────────────
bold="\033[1m"; dim="\033[2m"; green="\033[92m"; red="\033[91m"
yellow="\033[93m"; cyan="\033[96m"; reset="\033[0m"
say()  { echo -e "${bold}${cyan}▶  $*${reset}"; }
ok()   { echo -e "${green}✓  $*${reset}"; }
fail() { echo -e "${red}✗  $*${reset}"; }
dim()  { echo -e "${dim}   $*${reset}"; }

# ── check prerequisites ───────────────────────────────────────────────────────
if ! command -v pytest &>/dev/null; then
    fail "pytest not found. Run: pip install -r requirements.txt"
    exit 1
fi

if ! command -v docker &>/dev/null; then
    fail "docker not found"
    exit 1
fi

# ── check postgres is reachable ───────────────────────────────────────────────
check_postgres() {
    python3 -c "
import asyncio, asyncpg, sys
async def check():
    try:
        c = await asyncpg.connect('postgresql://taskuser:taskpass@localhost:5432/taskqueue')
        await c.close()
    except Exception as e:
        print(f'Cannot reach Postgres: {e}', file=sys.stderr)
        sys.exit(1)
asyncio.run(check())
" 2>&1
}

say "Checking PostgreSQL connection..."
if ! check_postgres; then
    fail "PostgreSQL is not reachable on localhost:5432"
    dim "Start with:  docker compose up -d postgres"
    exit 1
fi
ok "PostgreSQL is up"

# ── base pytest flags ─────────────────────────────────────────────────────────
BASE="pytest tests/ -p no:warnings"
TIMINGS="--durations=5"

# ── dispatch ──────────────────────────────────────────────────────────────────
MODE="${1:-all}"

case "$MODE" in

  all)
    say "Running ALL test suites..."
    echo ""
    for suite in test_enqueue test_exactly_once test_priority test_retry test_dlq test_stale_reaper; do
        echo -e "${bold}── $suite ──────────────────────────────────────────────${reset}"
        pytest "tests/${suite}.py" -v $TIMINGS || true
        echo ""
    done
    ;;

  api)
    say "Running API endpoint tests (test_enqueue.py)..."
    $BASE/test_enqueue.py -v $TIMINGS
    ;;

  once | exactly-once | exactly_once)
    say "Running exactly-once delivery test..."
    dim "5 workers race for 1 job — only 1 must win"
    $BASE/test_exactly_once.py -v $TIMINGS
    ;;

  priority)
    say "Running priority ordering tests..."
    dim "Jobs must complete in priority DESC order"
    $BASE/test_priority.py -v $TIMINGS
    ;;

  retry)
    say "Running retry/backoff tests..."
    dim "Flaky job: fail twice, succeed on attempt 2; run_at must advance"
    $BASE/test_retry.py -v $TIMINGS
    ;;

  dlq)
    say "Running dead-letter queue tests..."
    dim "always_fail with max_retries=3 → exactly 1 DLQ row, jobs.status=dead"
    $BASE/test_dlq.py -v $TIMINGS
    ;;

  reaper | stale | stale-reaper)
    say "Running stale-reaper tests..."
    dim "Expired heartbeat jobs → reset to pending; fresh jobs → untouched"
    $BASE/test_stale_reaper.py -v $TIMINGS
    ;;

  fast)
    say "Running all tests — stop on first failure (-x)..."
    $BASE -x -v $TIMINGS
    ;;

  verbose)
    say "Running all tests with full log output..."
    $BASE -v --log-cli-level=INFO $TIMINGS
    ;;

  coverage)
    say "Running all tests with coverage report..."
    if ! command -v coverage &>/dev/null; then
        fail "coverage not installed. Run: pip install coverage"
        exit 1
    fi
    coverage run -m pytest tests/ -v $TIMINGS
    coverage report -m
    coverage html -d htmlcov
    ok "HTML report written to htmlcov/index.html"
    ;;

  *)
    echo -e "${red}Unknown mode: ${MODE}${reset}"
    echo ""
    echo "Usage:  bash scripts/run_tests.sh [MODE]"
    echo ""
    echo "Modes:  all | api | once | priority | retry | dlq | reaper"
    echo "        fast | verbose | coverage"
    exit 1
    ;;

esac
