. (Join-Path $PSScriptRoot 'Common.ps1')
$runId = 'poc-' + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
Invoke-Kube -n $script:AirflowNamespace exec statefulset/airflow-poc-scheduler -- airflow dags unpause spring_batch_poc
Invoke-Kube -n $script:AirflowNamespace exec statefulset/airflow-poc-scheduler -- airflow dags trigger spring_batch_poc --run-id $runId
$deadline = [DateTime]::UtcNow.AddMinutes(10)
do {
    $state = & kubectl --context $script:KubeContext -n $script:BatchNamespace exec deployment/eod-postgres -- psql -U airflow -d airflow -Atc "SELECT state FROM dag_run WHERE dag_id='spring_batch_poc' AND run_id='$runId'"
    if ($LASTEXITCODE -ne 0) { throw 'Cannot query DAG state' }
    if ($state -eq 'failed') { throw "DAG failed: $runId; inspect Airflow task logs" }
    if ($state -eq 'success') { break }
    Start-Sleep -Seconds 10
} while ([DateTime]::UtcNow -lt $deadline)
if ($state -ne 'success') { throw "DAG timed out: $runId" }
Invoke-Kube -n $script:BatchNamespace exec deployment/eod-postgres -- psql -U airflow -d airflow -c 'SELECT business_date,tree_id,amount FROM spring_poc.exposures ORDER BY business_date,tree_id'
Invoke-Kube -n $script:BatchNamespace exec deployment/eod-postgres -- psql -U airflow -d airflow -c 'SELECT job_execution_id,status,exit_code FROM batch_job_execution ORDER BY job_execution_id DESC LIMIT 5'
Write-Host "DAG succeeded: $runId"
