. (Join-Path $PSScriptRoot 'Common.ps1')
& (Join-Path $PSScriptRoot 'Initialize-AirflowAccess.ps1')
# Enable Airflow's native login before publishing the route; no proxy Authorization conflict.
Invoke-Helm upgrade --install airflow-poc apache-airflow/airflow --version 1.22.0 --kube-context $script:KubeContext --namespace $script:AirflowNamespace -f (Join-Path $script:PocRoot 'airflow/values-local.yaml') --wait --timeout 10m
Invoke-Helm repo add traefik https://traefik.github.io/charts
Invoke-Helm upgrade --install traefik-poc traefik/traefik --version 42.2.0 --kube-context $script:KubeContext --namespace ingress-poc --create-namespace -f (Join-Path $script:PocRoot 'ingress/values-traefik.yaml') --wait --timeout 10m
Invoke-Helm upgrade --install airflow-ui (Join-Path $script:PocRoot 'charts/airflow-ingress') --kube-context $script:KubeContext --namespace $script:AirflowNamespace --wait --timeout 2m
Write-Host "Ingress configured: http://localhost/ . Credentials: $(Join-Path $script:LocalRoot 'ingress-credentials.json')"
