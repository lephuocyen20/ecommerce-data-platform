param(
    [ValidateSet('preflight', 'run', 'verify')]
    [string] $Mode = 'preflight',
    [string] $SilverArtifactManifest,
    [string] $GoldArtifactManifest
)

$ErrorActionPreference = 'Stop'

function Fail([string] $Message) { throw "LOCAL_E2E_CONTRACT_FAILED: $Message" }
function Require([bool] $Condition, [string] $Message) { if (-not $Condition) { Fail $Message } }
function Read-Manifest([string] $Path, [string] $Kind) {
    $resolved = (Resolve-Path -LiteralPath $Path -ErrorAction Stop).Path
    try { $document = Get-Content -LiteralPath $resolved -Raw -Encoding UTF8 | ConvertFrom-Json -ErrorAction Stop }
    catch { Fail "$Kind manifest is not valid JSON: $Path" }
    if ($null -eq $document -or $document -is [array]) { Fail "$Kind manifest must be a JSON object" }
    return $document
}
function Get-ListProperty($Document, [string[]] $Names, [string] $Kind) {
    foreach ($name in $Names) {
        $property = $Document.PSObject.Properties[$name]
        if ($null -ne $property -and $property.Value -is [array]) { return @($property.Value) }
    }
    Fail "$Kind manifest is missing its artifact list"
}
function Validate-Artifact([object] $Artifact, [string] $ExpectedTable, [string] $Kind) {
    if ($null -eq $Artifact -or $Artifact -is [string]) { Fail "$Kind artifact for $ExpectedTable is invalid" }
    $table = $Artifact.table
    if (-not $table) { $table = $Artifact.table_name }
    Require ($table -eq $ExpectedTable) "$Kind artifact table mismatch: expected $ExpectedTable"
    Require ($null -ne $Artifact.snapshot_id -and ([string]$Artifact.snapshot_id -match '^[1-9][0-9]*$')) "$Kind artifact $ExpectedTable needs a positive snapshot_id"
    $identity = $Artifact.identity
    if (-not $identity) { $identity = $Artifact.manifest_identity }
    Require ([string]$identity -match '^[A-Za-z0-9._:-]{8,256}$') "$Kind artifact $ExpectedTable needs an identity"
    $lineage = $Artifact.lineage
    if (-not $lineage) { $lineage = $Artifact.lineage_identity }
    if (-not $lineage) { $lineage = $Artifact.graph_identity }
    Require (-not [string]::IsNullOrWhiteSpace([string]$lineage)) "$Kind artifact $ExpectedTable is missing lineage"
    $evidence = $Artifact.evidence
    Require ($null -ne $evidence -and $evidence -is [pscustomobject]) "$Kind artifact $ExpectedTable evidence must be a non-empty object"
    $evidenceProperties = @($evidence.PSObject.Properties.Name | Sort-Object)
    Require ((($evidenceProperties -join '|') -ceq 'identity|snapshot_id|status|table')) "$Kind artifact $ExpectedTable evidence schema is invalid"
    Require (-not [string]::IsNullOrWhiteSpace([string]$evidence.identity)) "$Kind artifact $ExpectedTable evidence identity is empty"
    Require ([string]$evidence.status -ceq 'PASS') "$Kind artifact $ExpectedTable evidence status must be PASS"
    Require ([string]$evidence.table -ceq $ExpectedTable) "$Kind artifact $ExpectedTable evidence table is inconsistent"
    Require ([string]$evidence.snapshot_id -ceq [string]$Artifact.snapshot_id) "$Kind artifact $ExpectedTable evidence snapshot is inconsistent"
}
function Validate-Manifest([object] $Document, [string] $Kind, [string[]] $ExpectedTables) {
    Require ($Document.version -eq 1) "$Kind manifest version must be 1"
    $mode = if ($Document.mode) { [string]$Document.mode } elseif ($Document.DRY_RUN -eq $true) { 'DRY_RUN' } else { '' }
    Require ($mode -eq 'DRY_RUN') "$Kind manifest mode must be exactly DRY_RUN"
    Require ($Document.can_write -eq $false) "$Kind manifest can_write must be exactly false"
    $items = if ($Kind -eq 'Silver') { Get-ListProperty $Document @('silver_inputs','sources','artifacts') $Kind } else { Get-ListProperty $Document @('gold_outputs','outputs','artifacts') $Kind }
    Require ($items.Count -eq $ExpectedTables.Count) "$Kind manifest must contain exactly $($ExpectedTables.Count) artifacts"
    $seen = @{}
    foreach ($item in $items) {
        $table = if ($item.table) { [string]$item.table } else { [string]$item.table_name }
        Require (-not $seen.ContainsKey($table)) "$Kind manifest contains duplicate table $table"
        $seen[$table] = $true
    }
    Require ((@($seen.Keys | Sort-Object) -join '|') -ceq (@($ExpectedTables | Sort-Object) -join '|')) "$Kind manifest table set is incomplete or arbitrary"
    foreach ($table in $ExpectedTables) { Validate-Artifact ($items | Where-Object { (($_.table, $_.table_name) -contains $table) }) $table $Kind }
    Require ([string]$Document.run_id -eq $runId) "$Kind manifest run_id does not match QUALITY_IT_RUN_ID"
    Require ([string]$Document.nessie_ref -eq $nessieRef) "$Kind manifest nessie_ref does not match NESSIE_REF"
    return $items
}

$runId = $env:QUALITY_IT_RUN_ID
$bucket = if ($env:QUALITY_IT_BUCKET) { $env:QUALITY_IT_BUCKET } else { $env:BRONZE_BUCKET }
$prefix = $env:QUALITY_IT_PREFIX
if (-not $prefix) { $prefix = "quality-it/$runId/" }
$nessieUri = $env:NESSIE_URI
$nessieRef = if ($env:NESSIE_REF) { $env:NESSIE_REF } else { 'main' }

Require ($bucket -eq 'ecommerce-quality-it') 'bucket must be ecommerce-quality-it'
Require ($runId -match '^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$') 'QUALITY_IT_RUN_ID is invalid'
Require ($prefix -eq "quality-it/$runId/") 'prefix must be quality-it/<run-id>/'
Require ($nessieUri -match '^(http://)?(127\.0\.0\.1|localhost|nessie|host\.docker\.internal)(:\d+)?/api/v2$') 'NESSIE_URI must be a local Nessie /api/v2 endpoint'
Require ($nessieRef -match '^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$') 'NESSIE_REF is invalid'
Require ([string]$env:GOLD_RUN_MODE -ceq 'DRY_RUN') 'GOLD_RUN_MODE must be exactly DRY_RUN'
Require ([string]$env:GOLD_DIMENSIONAL_WRITE_ENABLED -ceq 'false') 'GOLD_DIMENSIONAL_WRITE_ENABLED must be exactly false'

$silver = @('customers', 'products', 'orders', 'order_items', 'payments', 'shipments')
$gold = @('dim_date', 'dim_customer', 'dim_product', 'fact_orders', 'fact_order_items', 'fact_payments', 'fact_shipments')
if ($Mode -in @('run', 'verify')) {
    Require (-not [string]::IsNullOrWhiteSpace($SilverArtifactManifest)) 'Silver artifact manifest is required for run/verify'
    Require (-not [string]::IsNullOrWhiteSpace($GoldArtifactManifest)) 'Gold artifact manifest is required for run/verify'
}
$silverDocument = $null; $goldDocument = $null
if (-not [string]::IsNullOrWhiteSpace($SilverArtifactManifest)) {
    Require (Test-Path -LiteralPath $SilverArtifactManifest -PathType Leaf) "Silver artifact manifest is missing: $SilverArtifactManifest"
    $silverDocument = Read-Manifest $SilverArtifactManifest 'Silver'
    if ($Mode -in @('run','verify')) { $silverItems = Validate-Manifest $silverDocument 'Silver' $silver }
}
if (-not [string]::IsNullOrWhiteSpace($GoldArtifactManifest)) {
    Require (Test-Path -LiteralPath $GoldArtifactManifest -PathType Leaf) "Gold artifact manifest is missing: $GoldArtifactManifest"
    $goldDocument = Read-Manifest $GoldArtifactManifest 'Gold'
    if ($Mode -in @('run','verify')) { $goldItems = Validate-Manifest $goldDocument 'Gold' $gold }
}

$result = [ordered]@{
    status = 'PRECHECK_PASS'; demo_mode = $Mode; mode = 'DRY_RUN'; write = 'false'; scope = 'LOCAL_READ_ONLY'; bucket = $bucket
    run_id = $runId; prefix = $prefix; nessie_uri = $nessieUri; nessie_ref = $nessieRef
    silver_inputs = $silver; silver_input_count = $silver.Count
    gold_outputs = $gold; gold_output_count = $gold.Count
    DRY_RUN = $true; can_write = $false; runtime_invoked = $false
    verification = if ($Mode -eq 'verify') { 'CONTRACT_ONLY_RUNTIME_NOT_INVOKED' } else { 'PREFLIGHT_ONLY' }
    manifest_validation = if ($Mode -in @('run','verify')) { 'PASS' } else { 'NOT_REQUESTED' }
}
$result | ConvertTo-Json -Depth 5
