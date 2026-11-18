[CmdletBinding()]
param(
    [string]$DagId = "ecommerce_incremental_ingestion",
    [int]$WindowMinutes = 60,
    [int]$MinimumSuccessfulRuns = 0,
    [string]$StartDateUtc = "",
    [string[]]$ExpectedComposeServices = @(
        "postgres",
        "minio",
        "nessie",
        "airflow-api-server",
        "airflow-scheduler",
        "airflow-dag-processor"
    )
)

$ErrorActionPreference = "Stop"

if ($WindowMinutes -lt 1) {
    throw "WindowMinutes must be at least 1."
}
if ($MinimumSuccessfulRuns -lt 0) {
    throw "MinimumSuccessfulRuns must be zero or greater."
}

function Invoke-AirflowCli {
    param([string[]]$Arguments)
    $previousPreference = $ErrorActionPreference
    $ErrorActionPreference = "Continue"
    $output = @(& docker compose exec -T airflow-scheduler airflow @Arguments 2>&1 | ForEach-Object { $_.ToString() })
    $exitCode = $LASTEXITCODE
    $ErrorActionPreference = $previousPreference
    if ($exitCode -ne 0) {
        throw ("Airflow CLI failed: {0}" -f ($output -join [Environment]::NewLine))
    }
    return $output
}

function Convert-JsonOutput {
    param(
        [string[]]$Output,
        [string]$Description
    )

    $lines = @($Output | ForEach-Object { $_.ToString().Trim() } | Where-Object { $_ })
    if ($lines.Count -eq 0) {
        throw ("{0} returned empty output; expected JSON." -f $Description)
    }

    $parsedItems = @()
    $emptyArraySeen = $false
    foreach ($line in $lines) {
        try {
            $parsed = $line | ConvertFrom-Json -ErrorAction Stop
        } catch {
            # Airflow emits structured log lines before its JSON payload. Ignore
            # those lines, but fail closed if no actual JSON payload is found.
            continue
        }
        if ($parsed -is [System.Collections.IEnumerable] -and -not ($parsed -is [string])) {
            if (@($parsed).Count -eq 0) {
                $emptyArraySeen = $true
                continue
            }
            $parsedItems += @($parsed)
        } else {
            $parsedItems += $parsed
        }
    }
    if ($parsedItems.Count -eq 0 -and $emptyArraySeen) {
        return [pscustomobject]@{ __empty_json_array = $true }
    }
    if ($parsedItems.Count -eq 0) {
        throw ("{0} returned malformed or missing JSON payload." -f $Description)
    }
    return $parsedItems
}

function Get-JsonArray {
    param(
        [object]$Value,
        [string]$Description
    )

    if ($null -eq $Value) {
        throw ("{0} returned null; expected a JSON array." -f $Description)
    }
    if ($Value.PSObject.Properties.Name -contains "__empty_json_array") {
        return @()
    }
    if ($Value -is [System.Collections.IEnumerable] -and
        -not ($Value -is [string]) -and
        -not ($Value.PSObject.Properties.Name -contains "items")) {
        return @($Value)
    }
    if ($Value.PSObject.Properties.Name -contains "items" -and
        $null -ne $Value.items -and
        $Value.items -is [System.Collections.IEnumerable]) {
        return @($Value.items)
    }
    throw ("{0} returned an unexpected JSON schema; expected an array." -f $Description)
}

function Get-RequiredProperty {
    param(
        [object]$Object,
        [string]$PropertyName,
        [string]$Description
    )

    $property = $Object.PSObject.Properties[$PropertyName]
    if ($null -eq $property -or [string]::IsNullOrWhiteSpace([string]$property.Value)) {
        throw ("{0} is missing required property '{1}'." -f $Description, $PropertyName)
    }
    return $property.Value
}

function Get-RunTimestamp {
    param([object]$Run)

    foreach ($propertyName in @("logical_date", "execution_date", "start_date")) {
        $property = $Run.PSObject.Properties[$propertyName]
        if ($null -ne $property -and -not [string]::IsNullOrWhiteSpace([string]$property.Value)) {
            $timestamp = [DateTimeOffset]::MinValue
            if ([DateTimeOffset]::TryParse([string]$property.Value, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::AssumeUniversal, [ref]$timestamp)) {
                return $timestamp.ToUniversalTime()
            }
            throw ("DAG run has invalid {0}: {1}" -f $propertyName, $property.Value)
        }
    }
    throw "DAG run is missing a parseable logical_date, execution_date, or start_date."
}

Write-Host "Checking Compose service health..."
$compose = & docker compose ps --format json 2>&1
if ($LASTEXITCODE -ne 0) {
    throw ("docker compose ps failed: {0}" -f ($compose -join [Environment]::NewLine))
}
$composeJson = Convert-JsonOutput -Output @($compose) -Description "docker compose ps"
$composeServices = Get-JsonArray -Value $composeJson -Description "docker compose ps"
$composeByService = @{}
foreach ($service in $composeServices) {
    $serviceName = Get-RequiredProperty -Object $service -PropertyName "Service" -Description "Compose service entry"
    $composeByService[[string]$serviceName] = $service
}
foreach ($expectedService in $ExpectedComposeServices) {
    if (-not $composeByService.ContainsKey($expectedService)) {
        throw "Required Compose service is missing: $expectedService"
    }
    $service = $composeByService[$expectedService]
    $state = [string](Get-RequiredProperty -Object $service -PropertyName "State" -Description "Compose service $expectedService")
    if ($state -notin @("running", "up")) {
        throw "Compose service $expectedService is not Up/running: $state"
    }
    $status = [string](Get-RequiredProperty -Object $service -PropertyName "Status" -Description "Compose service $expectedService")
    if ($status -notmatch '^Up(?:\s|$)') {
        throw "Compose service $expectedService has non-Up status: $status"
    }
    $healthProperty = $service.PSObject.Properties["Health"]
    if ($null -ne $healthProperty -and -not [string]::IsNullOrWhiteSpace([string]$healthProperty.Value) -and
        ([string]$healthProperty.Value).ToLowerInvariant() -ne "healthy") {
        throw "Compose service $expectedService is not healthy: $($healthProperty.Value)"
    }
}
$composeServices | ConvertTo-Json -Depth 10 | Write-Host

Write-Host "Checking Airflow DAG import errors..."
$importErrors = @(Invoke-AirflowCli @("dags", "list-import-errors", "--output", "json"))
$importJson = Convert-JsonOutput -Output $importErrors -Description "Airflow import errors"
$importEntries = Get-JsonArray -Value $importJson -Description "Airflow import errors"
if ($importEntries.Count -gt 0) {
    throw ("Airflow import errors reported: {0}" -f ($importEntries | ConvertTo-Json -Depth 10 -Compress))
}

Write-Host "Checking DAG registration..."
$dagList = @(Invoke-AirflowCli @("dags", "list", "--output", "json"))
$dagJson = Convert-JsonOutput -Output $dagList -Description "Airflow DAG list"
$dagEntries = Get-JsonArray -Value $dagJson -Description "Airflow DAG list"
$registeredDag = $dagEntries | Where-Object {
    $dagIdProperty = $_.PSObject.Properties["dag_id"]
    $null -ne $dagIdProperty -and [string]$dagIdProperty.Value -eq $DagId
}
if ($null -eq $registeredDag) {
    throw "DAG is not registered: $DagId"
}

if ([string]::IsNullOrWhiteSpace($StartDateUtc)) {
    $startDate = (Get-Date).ToUniversalTime().AddMinutes(-$WindowMinutes).ToString("o")
    Write-Host ("Run history for {0} in the last {1} minutes:" -f $DagId, $WindowMinutes)
} else {
    $startDate = $StartDateUtc
    Write-Host ("Run history for {0} since {1}:" -f $DagId, $StartDateUtc)
}
$runs = @(Invoke-AirflowCli @("dags", "list-runs", $DagId, "--start-date", $startDate, "--no-backfill", "--output", "json"))
$runsJson = Convert-JsonOutput -Output $runs -Description "Airflow DAG runs"
$runEntries = Get-JsonArray -Value $runsJson -Description "Airflow DAG runs"
if ($runEntries.Count -eq 0) {
    throw "No DAG run was observed in the requested observation window."
}

$windowStart = [DateTimeOffset]::Parse($startDate, [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::AssumeUniversal).ToUniversalTime()
$futureTimestampTolerance = [TimeSpan]::FromMinutes(5)
$nowUtc = [DateTimeOffset]::UtcNow
$parsedRuns = foreach ($run in $runEntries) {
    $runId = Get-RequiredProperty -Object $run -PropertyName "run_id" -Description "DAG run"
    $state = [string](Get-RequiredProperty -Object $run -PropertyName "state" -Description "DAG run $runId")
    if ($state -notin @("success", "failed", "running", "queued")) {
        throw "DAG run $runId has invalid state: $state"
    }
    $timestamp = Get-RunTimestamp -Run $run
    if ($timestamp -lt $windowStart) {
        throw "DAG run $runId is stale: $timestamp is before observation window $windowStart."
    }
    if ($timestamp -gt $nowUtc.Add($futureTimestampTolerance)) {
        throw "DAG run $runId has a future timestamp beyond the $($futureTimestampTolerance.TotalMinutes)-minute tolerance: $timestamp is after $($nowUtc.Add($futureTimestampTolerance))."
    }
    [pscustomobject]@{ Run = $run; RunId = $runId; State = $state; Timestamp = $timestamp }
}
$latestRun = $parsedRuns | Sort-Object Timestamp -Descending | Select-Object -First 1
if ($null -eq $latestRun) {
    throw "No valid DAG run was observed in the requested observation window."
}
if (@($parsedRuns | Where-Object State -eq "failed").Count -gt 0) {
    throw "Recent incremental DAG history contains a failed run. Follow docs/OBSERVABILITY.md before clearing or rerunning."
}
if (@($parsedRuns | Where-Object { $_.State -in @("queued", "running") }).Count -gt 0) {
    $incompleteRuns = @($parsedRuns | Where-Object { $_.State -in @("queued", "running") } | ForEach-Object {
        "{0}={1}" -f $_.RunId, $_.State
    }) -join ", "
    throw "Observation incomplete: DAG run(s) are still queued or running in the observation window: $incompleteRuns"
}

$successfulRuns = @($parsedRuns | Where-Object State -eq "success").Count
if ($successfulRuns -lt $MinimumSuccessfulRuns) {
    throw ("Observation incomplete: found {0} successful runs, need at least {1}." -f $successfulRuns, $MinimumSuccessfulRuns)
}

$parsedRuns | ForEach-Object { $_.Run } | ConvertTo-Json -Depth 10 | Write-Host
Write-Host "Pipeline health checks passed."
