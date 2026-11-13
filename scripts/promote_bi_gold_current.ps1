param(
    [Parameter(Mandatory = $true)] [string] $ManifestPath,
    [Parameter(Mandatory = $true)] [string] $CandidateHash,
    [string] $ExpectedCurrentHash,
    [switch] $Apply
)

$ErrorActionPreference = "Stop"
$candidateRef = "quality_it_run_20261112_gold_06"
$schema = "gold_run_20261112_gold_06"
$nessieUri = $env:TRINO_NESSIE_URI

if ([string]::IsNullOrWhiteSpace($nessieUri) -or
    $nessieUri -notmatch '^http://(localhost|127\.0\.0\.1|host\.docker\.internal)(:\d+)?/api/v2$') {
    throw "TRINO_NESSIE_URI must be a local HTTP Nessie /api/v2 endpoint."
}
if ($nessieUri -ne "http://host.docker.internal:19132/api/v2") {
    throw "TRINO_NESSIE_URI must be the certified run-06 endpoint."
}
if ($CandidateHash -notmatch '^[0-9a-fA-F]{8,128}$') {
    throw "CandidateHash must be a hexadecimal Nessie hash."
}
if (-not (Test-Path -LiteralPath $ManifestPath -PathType Leaf)) {
    throw "ManifestPath does not exist: $ManifestPath"
}
if ($Apply) {
    if ($env:BI_REF_PROMOTION_APPLY -ne "1") {
        throw "Apply requires BI_REF_PROMOTION_APPLY=1 as the explicit local guard."
    }
    if ([string]::IsNullOrWhiteSpace($ExpectedCurrentHash) -or
        ($ExpectedCurrentHash -ne 'ABSENT' -and $ExpectedCurrentHash -notmatch '^[0-9a-fA-F]{8,128}$')) {
        throw "Apply requires ExpectedCurrentHash as a hexadecimal hash or exact ABSENT for a missing target CAS."
    }
}

$python = Join-Path (Split-Path -Parent $PSScriptRoot) ".venv\Scripts\python.exe"
if (-not (Test-Path -LiteralPath $python -PathType Leaf)) { $python = "python" }
$arguments = @(
    "-m", "gold.implementation.bi_ref_promotion",
    "--manifest", $ManifestPath,
    "--candidate-ref", $candidateRef,
    "--candidate-hash", $CandidateHash,
    "--schema", $schema,
    "--nessie-uri", $nessieUri
)
if ($ExpectedCurrentHash) { $arguments += @("--expected-current-hash", $ExpectedCurrentHash) }
if ($Apply) { $arguments += "--apply" }
& $python @arguments
exit $LASTEXITCODE
