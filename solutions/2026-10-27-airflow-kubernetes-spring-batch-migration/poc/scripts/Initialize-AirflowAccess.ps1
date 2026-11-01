. (Join-Path $PSScriptRoot 'Common.ps1')
New-Item -ItemType Directory -Force -Path $script:LocalRoot | Out-Null
$credentialPath = Join-Path $script:LocalRoot 'ingress-credentials.json'
if (Test-Path -LiteralPath $credentialPath) {
    $credential = Get-Content -Raw -LiteralPath $credentialPath | ConvertFrom-Json
} else {
    $credential = [pscustomobject]@{username='poc';password=New-PocToken;url='http://localhost/'}
    $credential | ConvertTo-Json | Set-Content -Encoding utf8 -LiteralPath $credentialPath
}
$passwords = @{$credential.username=$credential.password} | ConvertTo-Json -Compress
Set-PocSecret $script:AirflowNamespace 'eod-ui-login' @{passwords=$passwords}
