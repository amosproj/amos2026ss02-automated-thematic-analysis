#!/usr/bin/env bash
# .env bootstrap helpers — must be sourced after common.sh.
# Relies on $SCRIPT_DIR being set in the sourcing script.

ensure_env_file() {
  local env_file="$SCRIPT_DIR/Backend/.env"
  local env_example="$SCRIPT_DIR/Backend/.env.example"

  if [[ -f "$env_file" ]]; then
    log_info ".env already exists — skipping copy"
    return 0
  fi

  [[ -f "$env_example" ]] \
    || die "Template $env_example not found. Is the repository fully checked out?"

  cp "$env_example" "$env_file"
  log_success "Created Backend/.env from Backend/.env.example"
  log_warn  "Set LLM_API_KEY in Backend/.env before using LLM-dependent features"
}

scan_env_placeholders() {
  local env_file="$SCRIPT_DIR/Backend/.env"

  if grep -qF '<your_api_key_here>' "$env_file" 2>/dev/null; then
    log_warn "LLM_API_KEY is still the placeholder value in Backend/.env"
    log_warn "LLM-dependent endpoints will return errors until a real key is set"
  fi
}