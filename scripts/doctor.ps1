# scripts/doctor.ps1 - Health check môi trường hệ thống
$ErrorActionPreference = 'SilentlyContinue'

Write-Host "===================================================="
Write-Host "  JOBMARKET INTELLIGENCE — SYSTEM DOCTOR REPORT"
Write-Host "  Time: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
Write-Host "  Hostname: $env:COMPUTERNAME"
Write-Host "===================================================="

Write-Host "`n[1] HE THONG & PHAN CUNG:"
$os = Get-CimInstance Win32_OperatingSystem
$totalRamGB = [math]::Round($os.TotalVisibleMemorySize / 1MB, 2)
$freeRamGB = [math]::Round($os.FreePhysicalMemory / 1MB, 2)
Write-Host "  OS: $($os.Caption) ($($os.Version)) - $($os.OSArchitecture)"
Write-Host "  RAM Tong: $totalRamGB GB | RAM Trong: $freeRamGB GB"

$drive = Get-PSDrive -Name C
$freeDiskGB = [math]::Round($drive.Free / 1GB, 2)
Write-Host "  O dia C: Con trong $freeDiskGB GB"

Write-Host "`n[2] TRANG THAI WSL2:"
$wslStatus = wsl --status 2>&1 | Out-String
if ($wslStatus -match "Default Version:\s*2") {
    Write-Host "  WSL2 Kernel: Da bat (Default Version 2)"
} else {
    Write-Host "  WSL2: $wslStatus"
}
$wslList = wsl --list --verbose 2>&1 | Out-String
Write-Host "  Danh sach Distro:"
Write-Host $wslList.Trim()

Write-Host "`n[3] RUNTIMES HIEN CO:"
$javaVer = (java -version 2>&1 | Select-Object -First 1)
Write-Host "  Java: $javaVer"

$pyVer = (python --version 2>&1)
Write-Host "  Python: $pyVer"

$nodeVer = (node -v 2>&1)
Write-Host "  Node.js: $nodeVer"

$gitVer = (git --version 2>&1)
Write-Host "  Git: $gitVer"

Write-Host "`n[4] DATASET GOC (VietJobs.csv):"
$csvPath = Join-Path $PSScriptRoot "..\VietJobs.csv"
if (Test-Path $csvPath) {
    $item = Get-Item $csvPath
    $sizeMB = [math]::Round($item.Length / 1MB, 2)
    Write-Host "  Tim thay: $($item.FullName)"
    Write-Host "  Dung luong: $sizeMB MB ($($item.Length) bytes)"
} else {
    Write-Host "  CHUA TIM THAY: VietJobs.csv tai thu muc goc repo!"
}

Write-Host "`n===================================================="
Write-Host "  DANH GIA DIEU KIEN:"
if ($totalRamGB -ge 8) {
    Write-Host "  [OK] RAM hop le cho Big Data (>= 8GB)."
} else {
    Write-Host "  [CANH BAO] RAM duoi 8GB, can cau hinh Spark/Hadoop sieu nhe."
}
if ($freeDiskGB -ge 15) {
    Write-Host "  [OK] Dung luong o dia con trong tot (>= 15GB)."
} else {
    Write-Host "  [CANH BAO] O dia con it, can don dep them."
}
Write-Host "===================================================="
