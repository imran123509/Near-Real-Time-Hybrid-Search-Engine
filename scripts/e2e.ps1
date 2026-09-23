<#
.SYNOPSIS
    Runs the end-to-end suite against the Docker Compose stack.

.DESCRIPTION
    1. checks that Docker is running and .env exists
    2. starts the stack, with the deterministic fake embedding provider so no
       API key is needed
    3. makes sure the Debezium connector is registered
    4. runs the E2E tests, which wait for every service to be ready
    5. prints the tail of the service logs if they fail
    6. leaves the stack running, unless -Down

    It never removes volumes or touches Docker resources outside this project.

.PARAMETER Down
    Stop the stack afterwards. Data volumes are kept; use
    "docker compose down -v" by hand to delete them.

.PARAMETER RealEmbedding
    Use the configured real embedding provider instead of the fake one, and
    run the opt-in semantic smoke test. Needs EMBEDDING_API_KEY in .env.

.PARAMETER Timeout
    Timeout passed to go test. Default 15m.

.EXAMPLE
    ./scripts/e2e.ps1

.EXAMPLE
    ./scripts/e2e.ps1 -RealEmbedding -Down
#>
param(
    [switch]$Down,
    [switch]$RealEmbedding,
    [string]$Timeout = '15m'
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
$testExit = 1
try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker does not answer. Start Docker Desktop and try again.'
    }
    if (-not (Test-Path .env)) {
        throw 'No .env file. Copy .env.example to .env first.'
    }

    if ($RealEmbedding) {
        $env:E2E_REAL_EMBEDDING = 'true'
        Write-Host 'Using the configured embedding provider (EMBEDDING_API_KEY must be set).'
    }
    else {
        # Deterministic local vectors: no API key, no network, no cost.
        $env:EMBEDDING_PROVIDER = 'fake'
        Write-Host 'Using the fake embedding provider.'
    }

    Write-Host 'Starting the stack...'
    docker compose up -d --build
    if ($LASTEXITCODE -ne 0) { throw 'docker compose up failed.' }

    # Idempotent: creates the connector, or updates it if it already exists.
    Write-Host 'Ensuring the Debezium connector is registered...'
    docker compose run --rm debezium-register
    if ($LASTEXITCODE -ne 0) { throw 'Registering the Debezium connector failed.' }

    # The tests wait for each service to be ready before they touch anything.
    $env:E2E = '1'
    Write-Host 'Running the end-to-end tests...'
    go test ./tests/e2e -v -timeout $Timeout
    $testExit = $LASTEXITCODE

    if ($testExit -ne 0) {
        Write-Host 'Tests failed; last 200 log lines per service:' -ForegroundColor Yellow
        docker compose logs --tail=200 --no-color consumer api debezium kafka
    }
}
finally {
    if ($Down) {
        Write-Host 'Stopping the stack (volumes are kept)...'
        docker compose down
    }
    Pop-Location
}

exit $testExit
