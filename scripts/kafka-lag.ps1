<#
.SYNOPSIS
    Prints the consumer group's lag, using Kafka's own tooling.

.DESCRIPTION
    Lag is how many messages have been produced that the group has not
    committed yet. It is the honest measure of whether the pipeline keeps up:
    a consumer that is falling behind still reports healthy throughput,
    because it is busy, and only the growing gap shows that work is arriving
    faster than it leaves.

      lag stays near zero   the consumer keeps up
      lag grows             the producer is faster; Kafka is holding the backlog
      lag shrinks           the consumer is catching up

    This runs kafka-consumer-groups.sh inside the broker container, so it
    needs nothing installed on the host.

.PARAMETER Group
    Consumer group. Default: near-realtime-search.

.PARAMETER Watch
    Keep printing until Ctrl+C.

.PARAMETER Interval
    Seconds between prints when watching. Default 2.

.EXAMPLE
    ./scripts/kafka-lag.ps1

.EXAMPLE
    ./scripts/kafka-lag.ps1 -Watch
#>
param(
    [string]$Group = 'near-realtime-search',
    [switch]$Watch,
    [int]$Interval = 2
)

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Parent $PSScriptRoot
Push-Location $repoRoot
try {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        throw 'Docker does not answer. Start Docker Desktop and try again.'
    }

    do {
        Write-Host ("--- {0} ---" -f (Get-Date -Format 'HH:mm:ss'))
        docker compose exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh `
            --bootstrap-server kafka:9092 --describe --group $Group
        if ($Watch) { Start-Sleep -Seconds $Interval }
    } while ($Watch)
}
finally {
    Pop-Location
}
