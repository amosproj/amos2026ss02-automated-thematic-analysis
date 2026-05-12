#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_DIR="$SCRIPT_DIR/Backend"
APP_PORT="${APP_PORT:-8000}"

# shellcheck source=scripts/lib/common.sh
source "$SCRIPT_DIR/scripts/lib/common.sh"
# shellcheck source=scripts/lib/prereqs.sh
source "$SCRIPT_DIR/scripts/lib/prereqs.sh"
# shellcheck source=scripts/lib/env.sh
source "$SCRIPT_DIR/scripts/lib/env.sh"

trap 'on_error $? $LINENO' ERR

# ── Defaults ──────────────────────────────────────────────────────────────────
MODE="up"
BUILD=true
REBUILD=false
DETACH=true
CONFIRM_YES=false
EXTRA_ARGS=()

# ── Usage ─────────────────────────────────────────────────────────────────────
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS] [-- EXTRA_PYTEST_ARGS...]

One-command bootstrap for the Automated Thematic Analysis stack.

OPTIONS
  -h, --help           Show this help and exit
  -d, --detach         Run containers in the background (default)
  -f, --foreground     Stream container logs to the terminal
      --no-build       Skip image rebuild (use cached images)
      --rebuild        Force full image rebuild (--no-cache)
      --test           Run the pytest test suite inside Docker
      --down           Stop and remove containers (keep data volumes)
      --down-volumes   Stop and remove containers AND data volumes
  -y, --yes            Skip confirmation prompts (use with --down-volumes)

ENVIRONMENT
  APP_PORT             Override the API host port (default: 8000)

EXAMPLES
  ./setup.sh                       Build and start the full stack
  ./setup.sh --foreground          Start and tail logs in the terminal
  ./setup.sh --no-build            Start without rebuilding images
  ./setup.sh --test                Run the full pytest suite
  ./setup.sh --test -- -k health   Run only tests matching 'health'
  ./setup.sh --down                Stop containers (keep data)
  ./setup.sh --down-volumes -y     Stop containers and delete Postgres data
EOF
}

# ── Argument parsing ──────────────────────────────────────────────────────────
PARSING_MAIN=true
for arg in "$@"; do
  if $PARSING_MAIN; then
    case "$arg" in
      -h|--help)        usage; exit 0 ;;
      -d|--detach)      DETACH=true ;;
      -f|--foreground)  DETACH=false ;;
      --no-build)       BUILD=false ;;
      --rebuild)        REBUILD=true ;;
      --test)           MODE="test" ;;
      --down)           MODE="down" ;;
      --down-volumes)   MODE="down-volumes" ;;
      -y|--yes)         CONFIRM_YES=true ;;
      --)               PARSING_MAIN=false ;;
      *)                log_error "Unknown option: $arg"; printf "\n"; usage; exit 1 ;;
    esac
  else
    EXTRA_ARGS+=("$arg")
  fi
done

# ── Compose wrapper ───────────────────────────────────────────────────────────
# All mode functions run after `cd "$COMPOSE_DIR"`, so docker compose
# picks up docker-compose.yml from Backend/ automatically.
run_compose() {
  docker compose "$@"
}

# ── Mode: down ────────────────────────────────────────────────────────────────
mode_down() {
  local remove_volumes=${1:-false}

  log_info "Stopping containers..."

  if $remove_volumes; then
    if ! $CONFIRM_YES; then
      printf "${YELLOW}[WARN]${RESET}    This will DELETE the Postgres data volume. Continue? [y/N] "
      read -r answer
      [[ "$answer" =~ ^[Yy]$ ]] || { log_info "Aborted — no changes made."; exit 0; }
    fi
    run_compose down -v
    log_success "Containers stopped and data volumes removed"
  else
    run_compose down
    log_success "Containers stopped (data volumes preserved)"
  fi
}

# ── Mode: test ────────────────────────────────────────────────────────────────
mode_test() {
  log_info "Running test suite inside Docker..."

  local pytest_cmd=(pytest --cov=app --cov-report=term-missing --cov-report=html)
  (( ${#EXTRA_ARGS[@]} > 0 )) && pytest_cmd+=("${EXTRA_ARGS[@]}")

  run_compose --profile test run --rm api-test "${pytest_cmd[@]}"
  log_success "Tests complete. Open Backend/htmlcov/index.html for the coverage report."
}

# ── Mode: up ──────────────────────────────────────────────────────────────────
mode_up() {
  log_info "Starting the stack..."

  local up_flags=()
  $DETACH && up_flags+=("-d")
  if $REBUILD; then
    up_flags+=(--build --no-cache)
  elif $BUILD; then
    up_flags+=(--build)
  fi

  if $BUILD || $REBUILD; then
    log_info "Building images — first run can take 3–5 minutes..."
  fi

  run_compose up "${up_flags[@]}"

  # In foreground mode Compose streams logs until Ctrl+C; nothing more to do.
  $DETACH || return 0

  log_info "Waiting for API to become ready (up to 60s)..."
  local health_url="http://localhost:${APP_PORT}/api/v1/health/ready"

  if wait_for_http "$health_url" 60; then
    print_success_banner
  else
    # Diagnose: distinguish exited container vs slow startup
    local running
    running=$(run_compose ps --status running --quiet api 2>/dev/null || true)
    if [[ -z "$running" ]]; then
      log_error "The 'api' container exited unexpectedly."
    else
      log_error "API container is running but health check timed out (60s)."
    fi
    log_error "Inspect logs with: docker compose logs api  (run from the Backend/ directory)"
    exit 1
  fi
}

print_success_banner() {
  printf "\n"
  printf "${GREEN}${BOLD}╔══════════════════════════════════════════════╗${RESET}\n"
  printf "${GREEN}${BOLD}║        Stack is up and healthy!              ║${RESET}\n"
  printf "${GREEN}${BOLD}╚══════════════════════════════════════════════╝${RESET}\n"
  printf "\n"
  printf "  API server   ${BOLD}http://localhost:${APP_PORT}${RESET}\n"
  printf "  API docs     ${BOLD}http://localhost:${APP_PORT}/docs${RESET}\n"
  printf "  Postgres     ${BOLD}localhost:5433${RESET}\n"
  printf "\n"
  printf "Next steps:\n"
  printf "  Tail logs    ${BOLD}docker compose logs -f api${RESET}   (from Backend/ directory)\n"
  printf "  Run tests    ${BOLD}./setup.sh --test${RESET}\n"
  printf "  Stop stack   ${BOLD}./setup.sh --down${RESET}\n"
  printf "\n"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  printf "${BOLD}Automated Thematic Analysis — bootstrap${RESET}\n\n"

  require_docker
  require_compose_v2
  printf "\n"

  ensure_env_file
  scan_env_placeholders
  printf "\n"

  cd "$COMPOSE_DIR"

  case "$MODE" in
    up)           mode_up ;;
    test)         mode_test ;;
    down)         mode_down false ;;
    down-volumes) mode_down true ;;
  esac
}

main