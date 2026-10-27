[CmdletBinding()]
param(
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')][string]$BusinessDate='2026-10-26',
    [ValidatePattern('^[A-Za-z0-9_-]{1,40}$')][string]$InputVersion='demo-v1'
)
. (Join-Path $PSScriptRoot 'Common.ps1')
$jobName = 'eod-seed-' + [DateTimeOffset]::UtcNow.ToUnixTimeSeconds()
$job = @{
    apiVersion='batch/v1';kind='Job';metadata=@{name=$jobName;namespace=$script:BatchNamespace}
    spec=@{backoffLimit=1;activeDeadlineSeconds=300;ttlSecondsAfterFinished=3600;template=@{spec=@{
        restartPolicy='Never';serviceAccountName='eod-batch';automountServiceAccountToken=$false
        containers=@(@{name='seed';image='apache/airflow:3.2.2';imagePullPolicy='IfNotPresent';command=@('python','/opt/eod/worker.py','seed')
            env=@(@{name='BUSINESS_DATE';value=$BusinessDate},@{name='INPUT_VERSION';value=$InputVersion},@{name='S3_ENDPOINT';value='http://eod-minio.batch-poc.svc.cluster.local:9000'},@{name='AWS_DEFAULT_REGION';value='us-east-1'},@{name='AWS_EC2_METADATA_DISABLED';value='true'})
            envFrom=@(@{secretRef=@{name='eod-worker-env'}})
            resources=@{requests=@{cpu='100m';memory='128Mi'};limits=@{cpu='500m';memory='512Mi'}}
            volumeMounts=@(@{name='worker';mountPath='/opt/eod';readOnly=$true})})
        volumes=@(@{name='worker';configMap=@{name='eod-worker'}})
    }}}
}
$job | ConvertTo-Json -Depth 20 -Compress | & kubectl --context $script:KubeContext apply -f -
if ($LASTEXITCODE -ne 0) { throw 'Seed job creation failed' }
Invoke-Kube -n $script:BatchNamespace wait --for=condition=complete "job/$jobName" --timeout=300s
Invoke-Kube -n $script:BatchNamespace logs "job/$jobName"
