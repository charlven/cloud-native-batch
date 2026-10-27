. (Join-Path $PSScriptRoot 'Common.ps1')
Write-Host 'Open http://localhost:8080 . Local POC uses automatic admin access on loopback only.'
Invoke-Kube -n $script:AirflowNamespace port-forward --address 127.0.0.1 service/airflow-poc-api-server 8080:8080
