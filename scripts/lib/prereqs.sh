#!/usr/bin/env bash
# Prerequisite checks — must be sourced after common.sh.

require_docker() {
  if ! command -v docker &>/dev/null; then
    die "Docker is not installed. Get it at: https://docs.docker.com/get-docker/"
  fi

  if ! docker version &>/dev/null 2>&1; then
    die "Docker daemon is not running. Start Docker Desktop or run: sudo systemctl start docker"
  fi

  log_success "Docker is available"
}

require_compose_v2() {
  if ! docker compose version &>/dev/null 2>&1; then
    die "Docker Compose v2 (the 'docker compose' subcommand) is required. Update Docker Desktop or see: https://docs.docker.com/compose/install/"
  fi

  log_success "Docker Compose v2 is available"
}