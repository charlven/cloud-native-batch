. (Join-Path $PSScriptRoot 'Common.ps1')
docker build -t eod-spring-batch:0.1.0 (Join-Path $script:PocRoot 'spring-batch')
if ($LASTEXITCODE -ne 0) { throw 'Spring Batch image build failed' }
Write-Host 'Image is available to the Docker Desktop cluster. Run Deploy-Dag.ps1 to publish the DAG.'
