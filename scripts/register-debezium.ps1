<#
.SYNOPSIS
    Registers the Debezium PostgreSQL connector, or updates it if it exists.

.DESCRIPTION
    Runs scripts/register-debezium.sh inside the debezium-register container,
    so Windows needs only Docker: no curl, jq or bash on the host, and one
    implementation of the registration logic for every platform.

    Docker Compose already runs this once on "docker compose up". Run it again
    after editing docker/debezium/connector.json. It is safe to repeat.

.EXAMPLE
    ./scripts/register-debezium.ps1
#>
$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    docker compose run --rm debezium-register
    if ($LASTEXITCODE -ne 0) {
        throw "Debezium connector registration failed (exit code $LASTEXITCODE). See the output above."
    }
}
finally {
    Pop-Location
}
