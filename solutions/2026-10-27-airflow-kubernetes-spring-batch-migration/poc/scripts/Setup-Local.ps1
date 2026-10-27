# Run using PowerShell 7. Never targets the current kubectl context implicitly.
[CmdletBinding()]
param()
. (Join-Path $PSScriptRoot 'Common.ps1')
New-Item -ItemType Directory -Force -Path $script:LocalRoot | Out-Null
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { throw 'Install Docker Desktop with WSL2 first.' }
docker info --format '{{.ServerVersion}}'
if ($LASTEXITCODE -ne 0) { throw 'Start Docker Desktop, then rerun this script.' }
Invoke-Kube wait --for=condition=Ready node/docker-desktop --timeout=120s

if (-not (Test-Path -LiteralPath $script:Helm)) {
    $toolRoot = Join-Path $script:LocalRoot 'tools'
    New-Item -ItemType Directory -Force -Path $toolRoot | Out-Null
    $archive = Join-Path $toolRoot 'helm.zip'
    Invoke-WebRequest 'https://get.helm.sh/helm-v3.19.0-windows-amd64.zip' -OutFile $archive
    $expected = ((Invoke-WebRequest 'https://get.helm.sh/helm-v3.19.0-windows-amd64.zip.sha256sum').Content -split '\s+')[0]
    if ((Get-FileHash $archive -Algorithm SHA256).Hash -ne $expected) { throw 'Helm checksum mismatch' }
    Expand-Archive -LiteralPath $archive -DestinationPath (Join-Path $toolRoot 'helm') -Force
}

foreach ($namespace in @($script:AirflowNamespace,$script:BatchNamespace)) {
    & kubectl --context $script:KubeContext create namespace $namespace --dry-run=client -o json | & kubectl --context $script:KubeContext apply -f -
    if ($LASTEXITCODE -ne 0) { throw 'Namespace creation failed' }
}

$statePath = Join-Path $script:LocalRoot 'eod-secrets.json'
if (Test-Path -LiteralPath $statePath) {
    $state = Get-Content -Raw -LiteralPath $statePath | ConvertFrom-Json
} else {
    $state = [pscustomobject]@{postgres=New-PocToken;access='eod-local';secret=New-PocToken;fernet=((New-PocToken)+'=');api=New-PocToken;jwt=New-PocToken}
    $state | ConvertTo-Json | Set-Content -LiteralPath $statePath -Encoding utf8
}
Set-PocSecret $script:BatchNamespace 'eod-credentials' @{'postgres-password'=$state.postgres;'s3-access-key'=$state.access;'s3-secret-key'=$state.secret}
Set-PocSecret $script:BatchNamespace 'eod-worker-env' @{AWS_ACCESS_KEY_ID=$state.access;AWS_SECRET_ACCESS_KEY=$state.secret;PGPASSWORD=$state.postgres}
Set-PocSecret $script:AirflowNamespace 'eod-airflow-metadata' @{connection="postgresql://airflow:$($state.postgres)@eod-postgres.batch-poc.svc.cluster.local:5432/airflow?sslmode=disable"}
$s3Connection = @{conn_type='aws';login=$state.access;password=$state.secret;extra=@{endpoint_url='http://eod-minio.batch-poc.svc.cluster.local:9000';region_name='us-east-1';config_kwargs=@{signature_version='s3v4';s3=@{addressing_style='path'}}}} | ConvertTo-Json -Depth 8 -Compress
Set-PocSecret $script:AirflowNamespace 'eod-airflow-connections' @{AIRFLOW_CONN_EOD_S3=$s3Connection}
Set-PocSecret $script:AirflowNamespace 'eod-fernet' @{'fernet-key'=$state.fernet}
Set-PocSecret $script:AirflowNamespace 'eod-api' @{'api-secret-key'=$state.api}
Set-PocSecret $script:AirflowNamespace 'eod-jwt' @{'jwt-secret'=$state.jwt}

Invoke-Helm repo add apache-airflow https://airflow.apache.org
Invoke-Helm upgrade --install eod-poc (Join-Path $script:PocRoot 'charts/eod-poc') --kube-context $script:KubeContext --namespace $script:BatchNamespace --wait --timeout 15m
Invoke-Helm upgrade --install airflow-poc apache-airflow/airflow --version 1.22.0 --kube-context $script:KubeContext --namespace $script:AirflowNamespace -f (Join-Path $script:PocRoot 'airflow/values-local.yaml') --wait --timeout 20m

# Deferred KPO tasks MUST continue occupying compute slots while pods run.
Invoke-Kube -n $script:AirflowNamespace exec deployment/airflow-poc-scheduler -- airflow pools set eod_compute 2 'Local EOD compute capacity' --include-deferred
& (Join-Path $PSScriptRoot 'Seed-Demo.ps1')
Write-Host 'Installed. Run Open-Airflow.ps1, then trigger eod_exposure_poc using the demo defaults.'
