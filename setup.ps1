#Requires -Version 5.1
<#
.SYNOPSIS
  One-command bootstrap for the Automated Thematic Analysis stack (Windows).

.DESCRIPTION
  Verifies prerequisites (Docker, Docker Compose v2), creates Backend\.env and
  Frontend\.env from templates if they do not exist, builds Docker images, starts
  the full stack (API + frontend), and polls both health endpoints until ready.

  Run this script from the repository root in PowerShell or Windows Terminal.
  Compatible with Windows PowerShell 5.1 and PowerShell 7+.

.PARAMETER Test
  Run the pytest test suite inside Docker.

.PARAMETER Lint
  Run linting checks (ruff and mypy) inside Docker.

.PARAMETER Down
  Stop and remove containers (data volumes are preserved).

.PARAMETER DownVolumes
  Stop and remove containers AND the Postgres data volume.

.PARAMETER Foreground
  Stream container output to the terminal instead of running detached.

.PARAMETER NoBuild
  Skip rebuilding images (use whatever is cached locally).

.PARAMETER Rebuild
  Force a full image rebuild with --no-cache.

.PARAMETER Yes
  Skip all confirmation prompts (use with -DownVolumes).

.EXAMPLE
  .\setup.ps1
  .\setup.ps1 -Test
  .\setup.ps1 -Lint
  .\setup.ps1 -Down
  .\setup.ps1 -DownVolumes -Yes
  .\setup.ps1 -Foreground
#>
[CmdletBinding()]
param(
  [switch]$Test,
  [switch]$Lint,
  [switch]$Down,
  [switch]$DownVolumes,
  [switch]$Foreground,
  [switch]$NoBuild,
  [switch]$Rebuild,
  [switch]$Yes,
  [Parameter(ValueFromRemainingArguments)]
  [string[]]$ExtraArgs
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$ScriptDir    = $PSScriptRoot
$ComposeDir   = $ScriptDir
$AppPort      = if ($env:APP_PORT)      { $env:APP_PORT }      else { '8000' }
$FrontendPort = if ($env:FRONTEND_PORT) { $env:FRONTEND_PORT } else { '3000' }

# Join-Path with more than two path arguments requires PowerShell 6+.
$BackendDir   = Join-Path $ScriptDir 'Backend'
$FrontendDir  = Join-Path $ScriptDir 'Frontend'
$EnvFile      = Join-Path $BackendDir  '.env'
$EnvExample   = Join-Path $BackendDir  '.env.example'
$FeEnvFile    = Join-Path $FrontendDir '.env'
$FeEnvExample = Join-Path $FrontendDir '.env.example'

# == Logging helpers ===========================================================
function Write-Info    { param([string]$Msg) Write-Host "[INFO]  $Msg" -ForegroundColor Cyan }
function Write-Ok      { param([string]$Msg) Write-Host "[OK]    $Msg" -ForegroundColor Green }
function Write-Warn    { param([string]$Msg) Write-Host "[WARN]  $Msg" -ForegroundColor Yellow }
function Write-Err     { param([string]$Msg) Write-Host "[ERROR] $Msg" -ForegroundColor Red }

function Exit-WithError {
  param([string]$Msg)
  Write-Err $Msg
  exit 1
}

# == Prerequisite checks =======================================================
function Assert-Docker {
  if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    Exit-WithError "Docker is not installed. Get it at: https://docs.docker.com/get-docker/"
  }

  $null = docker version 2>&1
  if ($LASTEXITCODE -ne 0) {
    Exit-WithError "Docker daemon is not running. Start Docker Desktop and try again."
  }

  Write-Ok "Docker is available"
}

function Assert-ComposeV2 {
  $null = docker compose version 2>&1
  if ($LASTEXITCODE -ne 0) {
    Exit-WithError "Docker Compose v2 (the 'docker compose' subcommand) is required. Update Docker Desktop or see: https://docs.docker.com/compose/install/"
  }

  Write-Ok "Docker Compose v2 is available"
}

# == .env bootstrap ============================================================
function Ensure-EnvFile {
  if (Test-Path $EnvFile) {
    Write-Info "Backend\.env already exists - skipping copy"
  }
  else {
    if (-not (Test-Path $EnvExample)) {
      Exit-WithError "Template $EnvExample not found. Is the repository fully checked out?"
    }
    Copy-Item $EnvExample $EnvFile
    Write-Ok   "Created Backend\.env from Backend\.env.example"
    Write-Warn "Set LLM_API_KEY in Backend\.env before using LLM-dependent features"
  }

  if (Test-Path $FeEnvFile) {
    Write-Info "Frontend\.env already exists - skipping copy"
  }
  elseif (Test-Path $FeEnvExample) {
    Copy-Item $FeEnvExample $FeEnvFile
    Write-Ok "Created Frontend\.env from Frontend\.env.example"
  }
  else {
    Write-Warn "Frontend\.env.example not found - skipping frontend env setup"
  }
}

function Test-EnvPlaceholders {
  if (Test-Path $EnvFile) {
    $content = Get-Content $EnvFile -Raw
    if ($content -match '<your_api_key_here>') {
      Write-Warn "LLM_API_KEY is still the placeholder value in Backend\.env"
      Write-Warn "LLM-dependent endpoints will return errors until a real key is set"
    }
  }
}

# == HTTP readiness poll =======================================================
function Wait-ForHttp {
  param(
    [string]$Url,
    [int]$MaxSeconds = 60
  )

  $elapsed  = 0
  $interval = 3

  while ($elapsed -lt $MaxSeconds) {
    try {
      $response = Invoke-WebRequest -Uri $Url -TimeoutSec 3 -UseBasicParsing `
                    -ErrorAction Stop
      if ($response.StatusCode -eq 200) { return $true }
    }
    catch {
      # Connection refused or non-200 - keep waiting
    }

    Start-Sleep -Seconds $interval
    $elapsed += $interval
  }

  return $false
}

# == Compose wrapper ===========================================================
# NOTE: $Args is a PowerShell automatic variable - do NOT use it as a param name.
function Invoke-Compose {
  param([string[]]$ComposeArgs)
  Push-Location $ComposeDir
  try {
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
      Exit-WithError "docker compose exited with code $LASTEXITCODE"
    }
  }
  finally {
    Pop-Location
  }
}

# == Mode: down ================================================================
function Invoke-Down {
  param([bool]$RemoveVolumes)

  Write-Info "Stopping containers..."

  if ($RemoveVolumes) {
    if (-not $Yes) {
      $answer = Read-Host "[WARN]  This will DELETE the Postgres data volume. Continue? [y/N]"
      if ($answer -notmatch '^[Yy]$') {
        Write-Info "Aborted - no changes made."
        exit 0
      }
    }
    Invoke-Compose @('down', '-v')
    Write-Ok "Containers stopped and data volumes removed"
  }
  else {
    Invoke-Compose @('down')
    Write-Ok "Containers stopped (data volumes preserved)"
  }
}

# == Mode: test ================================================================
function Invoke-Test {
  Write-Info "Running test suite inside Docker..."

  if ($Rebuild) {
    Write-Info "Rebuilding test images with --no-cache..."
    Invoke-Compose @('--profile', 'test', 'build', '--no-cache', 'api-test', 'frontend-test')
  }
  elseif (-not $NoBuild) {
    Write-Info "Ensuring test images are up to date..."
    Invoke-Compose @('--profile', 'test', 'build', 'api-test', 'frontend-test')
  }

  $apiPytestArgs = @(
    '--profile', 'test', 'run', '--rm', 'api-test',
    'pytest', '--cov=app', '--cov-report=term-missing', '--cov-report=html'
  )
  if ($ExtraArgs) { $apiPytestArgs += $ExtraArgs }

  Write-Info "Running backend tests..."
  Invoke-Compose $apiPytestArgs
  Write-Ok "Backend tests complete. Open Backend\htmlcov\index.html for the coverage report."

  Write-Info "Running frontend tests..."
  Invoke-Compose @('--profile', 'test', 'run', '--rm', 'frontend-test', 'pytest')
  Write-Ok "Frontend tests complete."
}

# == Mode: lint ================================================================
function Invoke-Lint {
  Write-Info "Running lint checks inside Docker..."

  $ruffArgs = @('--profile', 'test', 'run', '--rm', 'api-test', 'ruff', 'check', 'app', 'tests')
  Invoke-Compose $ruffArgs

  $mypyArgs = @('--profile', 'test', 'run', '--rm', 'api-test', 'mypy', 'app')
  Invoke-Compose $mypyArgs

  Write-Ok "Lint checks complete."
}

# == Mode: up ==================================================================
function Invoke-Up {
  Write-Info "Starting the stack..."

  $upFlags = [System.Collections.ArrayList]@()
  if (-not $Foreground) { $null = $upFlags.Add('-d') }
  if (-not $NoBuild)    { $null = $upFlags.Add('--build') }

  if ($Rebuild) {
    Write-Info "Rebuilding images with --no-cache..."
    Invoke-Compose @('build', '--no-cache')
  }
  elseif (-not $NoBuild) {
    Write-Info "Building images - first run can take 3-5 minutes..."
  }

  Invoke-Compose ([string[]](@('up') + $upFlags))

  # In foreground mode, Compose streams until Ctrl+C - nothing more to do.
  if ($Foreground) { return }

  Write-Info "Waiting for API to become ready (up to 60s)..."
  $apiHealthUrl = "http://localhost:${AppPort}/api/v1/health/ready"

  if (-not (Wait-ForHttp -Url $apiHealthUrl -MaxSeconds 60)) {
    Push-Location $ComposeDir
    $running = & docker compose ps --status running --quiet api 2>$null
    Pop-Location

    if ([string]::IsNullOrWhiteSpace($running)) {
      Write-Err "The 'api' container exited unexpectedly."
    }
    else {
      Write-Err "API container is running but health check timed out (60s)."
    }
    Write-Err "Inspect logs with: docker compose logs api"
    exit 1
  }

  Write-Info "Waiting for frontend to become ready (up to 60s)..."
  $feHealthUrl = "http://localhost:${FrontendPort}/health"

  if (Wait-ForHttp -Url $feHealthUrl -MaxSeconds 60) {
    Write-Host ""
    Write-Host "+----------------------------------------------+" -ForegroundColor Green
    Write-Host "|        Stack is up and healthy!              |" -ForegroundColor Green
    Write-Host "+----------------------------------------------+" -ForegroundColor Green
    Write-Host ""
    Write-Host "  Frontend UI  http://localhost:${FrontendPort}" -ForegroundColor White
    Write-Host "  API server   http://localhost:${AppPort}"      -ForegroundColor White
    Write-Host "  API docs     http://localhost:${AppPort}/docs"  -ForegroundColor White
    Write-Host "  Postgres     localhost:5433"                    -ForegroundColor White
    Write-Host ""
    Write-Host "Next steps:"
    Write-Host "  Tail logs    docker compose logs -f frontend api"
    Write-Host "  Run tests    .\setup.ps1 -Test"
    Write-Host "  Run lint     .\setup.ps1 -Lint"
    Write-Host "  Stop stack   .\setup.ps1 -Down"
    Write-Host ""
  }
  else {
    Push-Location $ComposeDir
    $feRunning = & docker compose ps --status running --quiet frontend 2>$null
    Pop-Location

    if ([string]::IsNullOrWhiteSpace($feRunning)) {
      Write-Err "The 'frontend' container exited unexpectedly."
    }
    else {
      Write-Err "Frontend container is running but health check timed out (60s)."
    }
    Write-Err "Inspect logs with: docker compose logs frontend"
    exit 1
  }
}

# == Main ======================================================================
Write-Host "Automated Thematic Analysis - bootstrap" -ForegroundColor White
Write-Host ""

Assert-Docker
Assert-ComposeV2
Write-Host ""

Ensure-EnvFile
Test-EnvPlaceholders
Write-Host ""

if ($DownVolumes) {
  Invoke-Down -RemoveVolumes $true
}
elseif ($Down) {
  Invoke-Down -RemoveVolumes $false
}
elseif ($Lint) {
  Invoke-Lint
}
elseif ($Test) {
  Invoke-Test
}
else {
  Invoke-Up
}

