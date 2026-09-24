<#
.SYNOPSIS
    Runs one benchmark suite against the local Docker Compose stack.

.DESCRIPTION
    Each suite answers one question, and they are kept apart on purpose:
    mixing them produces numbers nobody can interpret.

      search        HTTP search throughput and latency (k6)
      indexing      how fast changes travel from PostgreSQL into both indexes
      workers       the same indexing run at several INDEXING_WORKERS settings
      backpressure  produce faster than the consumer can process, and watch lag
      recovery      stop OpenSearch under load, bring it back, watch it recover

    The stack must already be running. Start it with the deterministic fake
    embedding provider, which is what makes a benchmark repeatable:

      $env:EMBEDDING_PROVIDER="fake"; docker compose up -d --build

    Nothing here removes containers or volumes. The rows a run creates are
    deleted by scripts/cleanup-loadtest.ps1, or by -Cleanup.

.PARAMETER Suite
    Which benchmark to run. Default: search.

.PARAMETER Scenario
    Search suite only: baseline, staged, limits, rps or validation.

.PARAMETER Count
    Indexing suite: how many documents to generate. Default 1000.

.PARAMETER Workers
    Workers suite: the INDEXING_WORKERS settings to compare.

.PARAMETER Rate
    Backpressure suite: events per second to publish.

.PARAMETER Runs
    How many times to repeat the suite. Report the median, not the best.

.PARAMETER Cleanup
    Delete the rows this run created when it finishes.

.EXAMPLE
    ./scripts/loadtest.ps1 -Suite search -Scenario baseline

.EXAMPLE
    ./scripts/loadtest.ps1 -Suite indexing -Count 5000 -Cleanup

.EXAMPLE
    ./scripts/loadtest.ps1 -Suite workers -Workers 1,4,16
#>
param(
    [ValidateSet('search', 'indexing', 'workers', 'backpressure', 'recovery')]
    [string]$Suite = 'search',

    [ValidateSet('baseline', 'staged', 'limits', 'rps', 'validation')]
    [string]$Scenario = 'baseline',

    [int]$Count = 1000,
    [int[]]$Workers = @(1, 2, 4, 8),
    [int]$Rate = 500,
    [int]$Runs = 1,
    [string]$RunId = (Get-Date -Format 'yyyyMMdd-HHmmss'),
    [switch]$Cleanup
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot

function Assert-Stack {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker does not answer. Start Docker Desktop and try again.'
    }
    $running = docker compose ps --services --filter status=running
    foreach ($service in @('postgres', 'kafka', 'opensearch', 'qdrant', 'api', 'consumer')) {
        if ($running -notcontains $service) {
            throw "The $service service is not running. Start the stack first: docker compose up -d"
        }
    }
    # A benchmark against the real embedding provider measures somebody else's
    # network and rate limits, and costs money per run.
    $provider = docker compose exec -T consumer printenv EMBEDDING_PROVIDER 2>$null
    if ($LASTEXITCODE -eq 0 -and $provider -and $provider.Trim() -ne 'fake') {
        Write-Host "WARNING: the consumer is using the '$($provider.Trim())' embedding provider." -ForegroundColor Yellow
        Write-Host '         Results will include its network latency and rate limits.' -ForegroundColor Yellow
        Write-Host '         Restart the stack with EMBEDDING_PROVIDER=fake for a reproducible run.' -ForegroundColor Yellow
    }
}

function Show-Environment {
    Write-Host ''
    Write-Host '--- environment ---------------------------------------------'
    Write-Host "date          $(Get-Date -Format 'u')"
    Write-Host "os            $([System.Environment]::OSVersion.VersionString)"
    Write-Host "cpus          $([System.Environment]::ProcessorCount)"
    try {
        $memGB = [math]::Round((Get-CimInstance Win32_ComputerSystem).TotalPhysicalMemory / 1GB, 1)
        Write-Host "memory        $memGB GB"
    }
    catch { }
    Write-Host "go            $(go version)"
    Write-Host "docker        $(docker --version)"
    Write-Host "compose       $(docker compose version --short)"
    Write-Host '(record these with the results: a number without its environment means nothing)'
    Write-Host '-------------------------------------------------------------'
    Write-Host ''
}

function Invoke-SearchSuite {
    if (-not (Get-Command k6 -ErrorAction SilentlyContinue)) {
        throw 'k6 is not installed. See benchmarks/README.md, or: winget install k6'
    }
    $env:LOADTEST_SCENARIO = $Scenario
    Write-Host "Running the k6 '$Scenario' scenario..." -ForegroundColor Cyan
    k6 run benchmarks/search/search.js
    if ($LASTEXITCODE -ne 0) {
        Write-Host 'k6 reported a failed threshold. The run is still valid data; read it.' -ForegroundColor Yellow
    }
}

function Invoke-IndexingSuite([string]$runId) {
    Write-Host "Running the indexing benchmark ($Count documents, run $runId)..." -ForegroundColor Cyan
    go run ./cmd/loadgen -mode index -run $runId -count $Count
    if ($LASTEXITCODE -ne 0) { throw 'The indexing benchmark failed.' }
}

function Invoke-WorkersSuite {
    Write-Host 'Comparing worker counts. Each step restarts the consumer.' -ForegroundColor Cyan
    Write-Host 'More workers is not automatically faster; the point is to find where it stops helping.'
    foreach ($count in $Workers) {
        Write-Host ''
        Write-Host "=== INDEXING_WORKERS=$count ===" -ForegroundColor Cyan
        $env:INDEXING_WORKERS = "$count"
        docker compose up -d consumer
        if ($LASTEXITCODE -ne 0) { throw 'Restarting the consumer failed.' }
        # Let it join the group and settle before anything is measured.
        Start-Sleep -Seconds 10

        Invoke-IndexingSuite "$RunId-w$count"
    }
    Remove-Item Env:INDEXING_WORKERS -ErrorAction SilentlyContinue
    Write-Host ''
    Write-Host 'Restoring the configured worker count...'
    docker compose up -d consumer
}

function Invoke-BackpressureSuite([string]$runId) {
    Write-Host "Publishing $Count events at $Rate events/sec, straight to Kafka." -ForegroundColor Cyan
    Write-Host 'Watch the lag column: if it grows, the consumer is behind, and Kafka is holding the backlog.'
    Write-Host 'Memory should stay flat, because the queue in the consumer is bounded.'
    Write-Host ''
    Write-Host 'In another terminal, watch the containers:  docker stats'
    Write-Host ''

    Start-Process -FilePath 'go' -ArgumentList "run ./cmd/loadgen -mode watch -run $runId -interval 2s -timeout 5m" -NoNewWindow
    go run ./cmd/loadgen -mode publish -run $runId -count $Count -rate $Rate
    if ($LASTEXITCODE -ne 0) { throw 'Publishing failed.' }

    Write-Host ''
    Write-Host 'Publishing finished. The watcher keeps printing until the lag is back to zero.'
    go run ./cmd/loadgen -mode watch -run $runId -interval 2s -timeout 3m
}

function Invoke-RecoverySuite([string]$runId) {
    Write-Host 'Stopping OpenSearch under load, then bringing it back.' -ForegroundColor Cyan
    Write-Host 'Expect failures, retries, and events in the dead-letter topic once the retries run out.'
    Write-Host ''

    docker compose stop opensearch
    if ($LASTEXITCODE -ne 0) { throw 'Stopping OpenSearch failed.' }
    try {
        go run ./cmd/loadgen -mode publish -run $runId -count $Count -rate $Rate
        Write-Host ''
        Write-Host 'Waiting while the consumer retries...'
        Start-Sleep -Seconds 20
    }
    finally {
        Write-Host 'Starting OpenSearch again...' -ForegroundColor Cyan
        docker compose start opensearch
    }

    $recoveryStart = Get-Date
    go run ./cmd/loadgen -mode watch -run $runId -interval 2s -timeout 5m
    Write-Host ("Recovery watched for {0:n0} seconds." -f ((Get-Date) - $recoveryStart).TotalSeconds)
    Write-Host 'Compare the dead-lettered count before and after; those events need a replay to reach the indexes.'
}

try {
    Assert-Stack
    Show-Environment

    for ($run = 1; $run -le $Runs; $run++) {
        if ($Runs -gt 1) {
            Write-Host ''
            Write-Host "########## run $run of $Runs ##########" -ForegroundColor Green
        }
        $runId = if ($Runs -gt 1) { "$RunId-r$run" } else { $RunId }

        switch ($Suite) {
            'search' { Invoke-SearchSuite }
            'indexing' { Invoke-IndexingSuite $runId }
            'workers' { Invoke-WorkersSuite }
            'backpressure' { Invoke-BackpressureSuite $runId }
            'recovery' { Invoke-RecoverySuite $runId }
        }
    }

    if ($Runs -gt 1) {
        Write-Host ''
        Write-Host 'Report the median of these runs, not the fastest one.' -ForegroundColor Yellow
    }
    Write-Host ''
    Write-Host "Record the results in benchmarks/results/README.md with the environment above." -ForegroundColor Green
}
finally {
    if ($Cleanup) {
        Write-Host ''
        Write-Host 'Deleting the rows this benchmark created...'
        go run ./cmd/loadgen -mode cleanup -run $RunId
    }
    Pop-Location
}
