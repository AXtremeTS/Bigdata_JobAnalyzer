# scripts/check-repo-size.ps1
# Checks git repository size to ensure compliance with the < 100MB constraint

$ErrorActionPreference = "Stop"
$projectRoot = Split-Path -Parent $PSScriptRoot

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host " [JobMarket Intelligence] Repository Size Audit" -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

# Calculate size of git tracked files
$gitFiles = git ls-files | ForEach-Object {
    $fullPath = Join-Path $projectRoot $_
    if (Test-Path $fullPath) {
        Get-Item $fullPath
    }
}

$totalBytes = ($gitFiles | Measure-Object -Property Length -Sum).Sum
$totalMB = [math]::Round($totalBytes / 1MB, 2)

Write-Host "Tracked Files Count: $($gitFiles.Count)" -ForegroundColor Yellow
Write-Host "Total Tracked Size : $totalMB MB (Constraint: < 100 MB)" -ForegroundColor $(if ($totalMB -lt 100) { "Green" } else { "Red" })

# Check for individual files > 10MB
$largeFiles = $gitFiles | Where-Object { $_.Length -gt 10MB }
if ($largeFiles) {
    Write-Host "`nWARNING: The following tracked files exceed 10MB:" -ForegroundColor Red
    $largeFiles | ForEach-Object {
        $sizeMB = [math]::Round($_.Length / 1MB, 2)
        Write-Host "  - $($_.FullName) ($sizeMB MB)" -ForegroundColor Red
    }
} else {
    Write-Host "`n[PASS] No individual tracked file exceeds 10MB." -ForegroundColor Green
}

if ($totalMB -lt 100) {
    Write-Host "`n[SUCCESS] Repository size is well within the 100MB limit ($totalMB MB / 100 MB)." -ForegroundColor Green
} else {
    Write-Host "`n[FAIL] Repository size exceeds 100MB!" -ForegroundColor Red
    exit 1
}
