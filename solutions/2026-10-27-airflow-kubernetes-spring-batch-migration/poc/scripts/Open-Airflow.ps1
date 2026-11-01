. (Join-Path $PSScriptRoot 'Common.ps1')
Write-Host 'Fallback port forward: http://localhost:8080 . Normal persistent access uses http://localhost/ via Ingress.'
Write-Host 'Airflow login credentials are in .local/ingress-credentials.json.'
Invoke-Kube -n $script:AirflowNamespace port-forward --address 127.0.0.1 service/airflow-poc-api-server 8080:8080
