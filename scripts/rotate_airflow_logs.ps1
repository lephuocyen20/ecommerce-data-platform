[CmdletBinding(SupportsShouldProcess)]
param(
    [int]$RetentionDays = 14,
    [int]$MinimumAgeHours = 24,
    [switch]$Apply
)

$ErrorActionPreference = "Stop"

if ($RetentionDays -lt 1) {
    throw "RetentionDays must be at least 1."
}
if ($MinimumAgeHours -lt 24) {
    throw "MinimumAgeHours must be at least 24 to protect the active observation window."
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot ".." )).Path
$logRoot = Join-Path $repoRoot "airflow\logs"
if (-not (Test-Path -LiteralPath $logRoot -PathType Container)) {
    throw "Airflow log root does not exist: $logRoot"
}

$retentionCutoff = (Get-Date).AddDays(-$RetentionDays)
$graceCutoff = (Get-Date).AddHours(-$MinimumAgeHours)
$cutoff = if ($retentionCutoff -lt $graceCutoff) { $retentionCutoff } else { $graceCutoff }
$candidates = @(Get-ChildItem -LiteralPath $logRoot -File -Recurse | Where-Object {
    $_.LastWriteTime -lt $cutoff
})

Write-Host ("Airflow log root: {0}" -f $logRoot)
Write-Host ("Retention cutoff: {0:O}" -f $retentionCutoff)
Write-Host ("Minimum-age safety cutoff: {0:O}" -f $graceCutoff)
Write-Host ("Candidate files: {0}" -f $candidates.Count)

foreach ($file in $candidates) {
    $relative = $file.FullName.Substring($logRoot.Length).TrimStart("\")
    if ($Apply) {
        if ($PSCmdlet.ShouldProcess($file.FullName, "Remove expired Airflow log")) {
            Remove-Item -LiteralPath $file.FullName -Force
        }
    } else {
        Write-Host ("DRY_RUN {0} ({1} bytes, last write {2:O})" -f $relative, $file.Length, $file.LastWriteTime)
    }
}

if (-not $Apply) {
    Write-Host "No files were removed. Re-run with -Apply after reviewing the list."
}
