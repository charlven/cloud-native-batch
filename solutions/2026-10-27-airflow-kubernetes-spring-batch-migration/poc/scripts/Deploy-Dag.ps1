. (Join-Path $PSScriptRoot 'Common.ps1')
Invoke-Helm lint (Join-Path $script:PocRoot 'charts/eod-poc')
Invoke-Helm upgrade --install eod-poc (Join-Path $script:PocRoot 'charts/eod-poc') --kube-context $script:KubeContext --namespace $script:BatchNamespace --wait --timeout 10m
Write-Host 'DAG ConfigMap updated. Allow one Airflow parsing interval for the new definition.'
