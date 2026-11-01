# Start a detached loopback-only forward; closing the invoking terminal will not stop it.
. (Join-Path $PSScriptRoot 'Common.ps1')
New-Item -ItemType Directory -Force -Path $script:LocalRoot | Out-Null
$listener = Get-NetTCPConnection -LocalPort 8080 -State Listen -ErrorAction SilentlyContinue
if ($listener) {
    Write-Host 'Port 8080 is already in use. Reuse the existing forward if it is Airflow; no process was stopped.'
    return
}
$kubectl = (Get-Command kubectl -ErrorAction Stop).Source
$forward = Start-Process -FilePath $kubectl -WindowStyle Hidden -PassThru -ArgumentList @(
    '--context', $script:KubeContext, '-n', $script:AirflowNamespace,
    'port-forward', '--address', '127.0.0.1', 'service/airflow-poc-api-server', '8080:8080'
) -RedirectStandardOutput (Join-Path $script:LocalRoot 'airflow-ui.stdout.log') -RedirectStandardError (Join-Path $script:LocalRoot 'airflow-ui.stderr.log')
$forward.Id | Set-Content (Join-Path $script:LocalRoot 'airflow-ui.pid')
for ($attempt = 0; $attempt -lt 10; $attempt++) {
    Start-Sleep -Seconds 1
    $forward.Refresh()
    if ($forward.HasExited) { throw 'Port forwarding exited; inspect .local/airflow-ui.stderr.log' }
    try {
        $response = Invoke-WebRequest -Uri 'http://127.0.0.1:8080/' -TimeoutSec 3 -NoProxy
        if ($response.StatusCode -eq 200) {
            Write-Host "Airflow ready: http://localhost:8080/ (forward PID $($forward.Id))"
            return
        }
    } catch { }
}
throw 'Forward started but UI did not respond; inspect .local/airflow-ui.stderr.log and cluster readiness.'
