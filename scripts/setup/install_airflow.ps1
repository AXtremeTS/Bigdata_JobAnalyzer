param(
    [string]$Python = "python"
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path "$PSScriptRoot\..\..").Path
$venv = Join-Path $projectRoot ".airflow-venv"
$requirements = Join-Path $projectRoot "requirements-airflow.txt"

Write-Host "Airflow installation"
Write-Host "Airflow is supported through WSL2/Linux; this script installs the pinned package into an isolated venv."
if (-not (Test-Path $requirements)) {
    throw "Missing $requirements"
}

if (-not (Test-Path (Join-Path $venv "Scripts\python.exe"))) {
    & $Python -m venv $venv
    if ($LASTEXITCODE -ne 0) { throw "Could not create Airflow virtual environment." }
}

$venvPython = Join-Path $venv "Scripts\python.exe"
& $venvPython -m pip install --upgrade pip
& $venvPython -m pip install -r $requirements
if ($LASTEXITCODE -ne 0) {
    throw "Airflow installation failed. On Windows, run this from WSL2 or Linux."
}

$env:AIRFLOW_HOME = Join-Path $projectRoot ".airflow"
New-Item -ItemType Directory -Force -Path (Join-Path $env:AIRFLOW_HOME "dags") | Out-Null
Copy-Item (Join-Path $projectRoot "airflow\dags\jobmarket_pipeline.py") `
    (Join-Path $env:AIRFLOW_HOME "dags\jobmarket_pipeline.py") -Force
& $venvPython -m airflow db migrate
& $venvPython -m airflow version
Write-Host "[SUCCESS] Airflow installed in $venv"
