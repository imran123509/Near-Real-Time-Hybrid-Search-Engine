<#
.SYNOPSIS
    Deletes the rows a benchmark created.

.DESCRIPTION
    Benchmark documents are identified by their generated URL prefix
    (https://example.com/loadtest/...), so this can only ever delete rows a
    benchmark made. Seeded and hand-written documents have different URLs and
    are never touched.

    Deleting the rows also removes them from OpenSearch and Qdrant, through
    the same delete events any other delete produces, so the indexes end up
    back where they started. Give it a moment to travel through the pipeline.

    Nothing here removes containers, volumes or images.

.PARAMETER RunId
    Delete only this run's rows. Without it, every benchmark run's rows are
    deleted.

.PARAMETER Confirm
    Skip the prompt.

.EXAMPLE
    ./scripts/cleanup-loadtest.ps1

.EXAMPLE
    ./scripts/cleanup-loadtest.ps1 -RunId 20260924-101500
#>
param(
    [string]$RunId = '',
    [switch]$Confirm
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker does not answer. Start Docker Desktop and try again.'
    }

    $scope = if ($RunId) { "run $RunId" } else { 'every benchmark run' }
    if (-not $Confirm) {
        $answer = Read-Host "Delete the benchmark rows of $scope? [y/N]"
        if ($answer -notmatch '^(y|yes)$') {
            Write-Host 'Nothing was deleted.'
            exit 0
        }
    }

    if ($RunId) {
        go run ./cmd/loadgen -mode cleanup -run $RunId
    }
    else {
        go run ./cmd/loadgen -mode cleanup -all
    }
    if ($LASTEXITCODE -ne 0) { throw 'Cleanup failed.' }

    Write-Host ''
    Write-Host 'Watch the indexes shrink as the deletes travel through the pipeline:'
    Write-Host '  go run ./cmd/loadgen -mode watch -interval 2s -timeout 2m'
}
finally {
    Pop-Location
}
