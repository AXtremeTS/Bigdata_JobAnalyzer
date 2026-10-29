param(
    [string]$Python = "$PSScriptRoot\..\.airflow-venv\Scripts\python.exe"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path "$PSScriptRoot\..").Path
$env:AIRFLOW_HOME = Join-Path $projectRoot ".airflow"
$env:JOBMARKET_HOME = $projectRoot
$logDir = Join-Path $projectRoot "docs\evidence\airflow"
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$logPath = Join-Path $logDir ("dag-test-{0}.log" -f (Get-Date -Format "yyyyMMdd-HHmmss"))

if (-not (Test-Path $Python)) {
    throw "Airflow Python not found at $Python. Run scripts\setup\install_airflow.ps1 first."
}

& $Python -m airflow dags test jobmarket_intelligence_pipeline (Get-Date -Format "yyyy-MM-dd") 2>&1 |
    Tee-Object -FilePath $logPath
if ($LASTEXITCODE -ne 0) { throw "Airflow DAG test failed. See $logPath" }
Write-Host "[SUCCESS] DAG evidence saved to $logPath"
