# 1_infrastructure/check_env.ps1
# Script kiểm tra chi tiết các biến môi trường Big Data

Write-Host "`n=================================================================" -ForegroundColor Cyan
Write-Host "   KIỂM TRA BIẾN MÔI TRƯỜNG BIG DATA (SYSTEM & USER ENVIRONMENT)" -ForegroundColor Yellow
Write-Host "=================================================================" -ForegroundColor Cyan

$vars = @("JAVA_HOME", "HADOOP_HOME", "SPARK_HOME", "KAFKA_HOME")

foreach ($v in $vars) {
    # 1. Kiểm tra session hiện tại
    $currentVal = [System.Environment]::GetEnvironmentVariable($v, "Process")
    # 2. Kiểm tra System (Toàn máy)
    $machineVal = [System.Environment]::GetEnvironmentVariable($v, "Machine")
    # 3. Kiểm tra User (Tài khoản)
    $userVal    = [System.Environment]::GetEnvironmentVariable($v, "User")

    $effective = if ($currentVal) { $currentVal } elseif ($machineVal) { $machineVal } elseif ($userVal) { $userVal } else { "(Chưa thiết lập)" }
    
    $color = if ($effective -ne "(Chưa thiết lập)") { "Green" } else { "DarkGray" }
    Write-Host ("  {0,-12} : {1}" -f $v, $effective) -ForegroundColor $color
}

Write-Host "`n-----------------------------------------------------------------" -ForegroundColor Cyan
Write-Host "   CÁC ĐƯỜNG DẪN LIÊN QUAN TRONG BIẾN PATH:" -ForegroundColor Yellow
Write-Host "-----------------------------------------------------------------" -ForegroundColor Cyan

$pathList = ([System.Environment]::GetEnvironmentVariable("PATH", "Process") -split ";")
$matched = 0

foreach ($p in $pathList) {
    if ($p -and ($p -match "java|hadoop|spark|kafka|bigdata")) {
        $exists = Test-Path $p
        $status = if ($exists) { "[TỒN TẠI]" } else { "[KHÔNG TỒN TẠI]" }
        $color = if ($exists) { "Green" } else { "Red" }
        Write-Host "  $status $p" -ForegroundColor $color
        $matched++
    }
}

if ($matched -eq 0) {
    Write-Host "  (Không tìm thấy đường dẫn Big Data trong PATH session này)" -ForegroundColor DarkGray
}

Write-Host "=================================================================`n" -ForegroundColor Cyan
