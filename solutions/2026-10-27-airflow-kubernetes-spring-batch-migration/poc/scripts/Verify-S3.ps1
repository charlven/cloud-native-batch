[CmdletBinding()]
param([ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$BusinessDate='2026-10-26')
. (Join-Path $PSScriptRoot 'Common.ps1')
$suffix = [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$inputVersion = "wait-proof-$suffix"
$runId = "s3-wait-proof-$suffix"
$conf = @{business_date=$BusinessDate;input_version=$inputVersion;config_version='demo-config-1'} | ConvertTo-Json -Compress
Invoke-Kube -n $script:AirflowNamespace exec statefulset/airflow-poc-scheduler -- airflow dags unpause eod_exposure_poc
Invoke-Kube -n $script:AirflowNamespace exec statefulset/airflow-poc-scheduler -- airflow dags trigger eod_exposure_poc --run-id $runId --conf $conf

function Read-State([string]$Sql) {
    $result = & kubectl --context $script:KubeContext -n $script:BatchNamespace exec deployment/eod-postgres -- psql -U airflow -d airflow -Atc $Sql
    if ($LASTEXITCODE -ne 0) { throw 'Could not query Airflow state' }
    return $result
}

# The unique object is deliberately absent: verify a real deferred wait first.
$deadline = [DateTime]::UtcNow.AddMinutes(4)
do {
    $state = Read-State "SELECT state FROM task_instance WHERE dag_id='eod_exposure_poc' AND run_id='$runId' AND task_id='wait_manifest'"
    if ($state -eq 'failed') { throw 'Sensor failed before entering deferred state' }
    if ($state -eq 'deferred') { break }
    Start-Sleep -Seconds 5
} while ([DateTime]::UtcNow -lt $deadline)
if ($state -ne 'deferred') { throw 'Sensor did not enter deferred state before timeout' }
Write-Host "Verified deferred wait for missing manifest: $inputVersion"
& (Join-Path $PSScriptRoot 'Seed-Demo.ps1') -BusinessDate $BusinessDate -InputVersion $inputVersion

$deadline = [DateTime]::UtcNow.AddMinutes(12)
do {
    $state = Read-State "SELECT state FROM dag_run WHERE dag_id='eod_exposure_poc' AND run_id='$runId'"
    if ($state -eq 'failed') { throw "S3 flow failed: $runId; inspect task logs" }
    if ($state -eq 'success') { break }
    Start-Sleep -Seconds 10
} while ([DateTime]::UtcNow -lt $deadline)
if ($state -ne 'success') { throw "S3 flow timed out: $runId" }
Invoke-Kube -n $script:BatchNamespace exec deployment/eod-postgres -- psql -U airflow -d airflow -c "SELECT stage,output_key FROM eod_demo.stage_results WHERE business_date='$BusinessDate' AND input_version='$inputVersion' ORDER BY stage"
Write-Host "S3 wait and complete pipeline succeeded: $runId"
