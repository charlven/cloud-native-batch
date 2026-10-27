$ErrorActionPreference = 'Stop'
$script:PocRoot = Split-Path -Parent $PSScriptRoot
$script:RepoRoot = [IO.Path]::GetFullPath((Join-Path $PSScriptRoot '../../../..'))
$script:LocalRoot = Join-Path $script:RepoRoot '.local'
$script:Helm = Join-Path $script:LocalRoot 'tools/helm/windows-amd64/helm.exe'
$script:KubeContext = 'docker-desktop'
$script:AirflowNamespace = 'airflow-poc'
$script:BatchNamespace = 'batch-poc'
# Only local cluster traffic bypasses the workstation's external network proxy.
$env:NO_PROXY = 'localhost,127.0.0.1,kubernetes.docker.internal,192.168.65.3,192.168.65.0/24,10.96.0.0/12,10.1.0.0/16,.svc,.cluster.local'
$env:no_proxy = $env:NO_PROXY
$script:HelmOptions = @('--repository-config', (Join-Path $script:LocalRoot 'helm/repositories.yaml'), '--repository-cache', (Join-Path $script:LocalRoot 'helm/cache'))

function Invoke-Kube {
    & kubectl --context $script:KubeContext @args
    if ($LASTEXITCODE -ne 0) { throw "kubectl failed (exit $LASTEXITCODE)" }
}

function Invoke-Helm {
    & $script:Helm @args @script:HelmOptions
    if ($LASTEXITCODE -ne 0) { throw "helm failed (exit $LASTEXITCODE)" }
}

function Set-PocSecret([string]$Namespace, [string]$Name, [hashtable]$Values) {
    $manifest = @{apiVersion='v1';kind='Secret';metadata=@{name=$Name;namespace=$Namespace};type='Opaque';stringData=$Values}
    $manifest | ConvertTo-Json -Depth 10 -Compress | & kubectl --context $script:KubeContext apply -f -
    if ($LASTEXITCODE -ne 0) { throw "Failed to apply secret $Name" }
}

function New-PocToken {
    $bytes = New-Object byte[] 32
    [Security.Cryptography.RandomNumberGenerator]::Fill($bytes)
    return [Convert]::ToBase64String($bytes).TrimEnd('=').Replace('+','-').Replace('/','_')
}
