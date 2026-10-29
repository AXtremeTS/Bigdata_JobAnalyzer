# run_all.ps1
<#
.SYNOPSIS
    Master Orchestration Script for JobMarket Intelligence Big Data Platform.
    Organized strictly according to the 4 Workflow Folders:
      [1] 1_infrastructure  - Start Hadoop HDFS (1 NameNode + 3 DataNodes)
      [2] 2_pipeline        - Execute 3-Tier Data Pipeline (Tang 1 -> Tang 2 -> Tang 3)
      [3] 3_backend         - Launch FastAPI REST Service (Port 8000)
      [4] 4_frontend        - Launch Vite React Dashboard (Port 5173)

.EXAMPLE
    .\run_all.ps1              # Runs the complete pipeline and launches all services
    .\run_all.ps1 -Step hdfs    # Starts HDFS 1 NameNode + 3 DataNodes only
    .\run_all.ps1 -Step pipeline# Runs the 3-tier pipeline only
    .\run_all.ps1 -Step backend # Starts FastAPI server only
    .\run_all.ps1 -Step frontend# Starts Vite React dashboard only
#>

param (
    [ValidateSet("all", "hdfs", "pipeline", "backend", "frontend", "status", "inventory", "stop")]
    [string]$Step = "all"
)

$ProjectRoot = $PSScriptRoot
Set-Location $ProjectRoot

# Colors
function Write-Header($text) {
    Write-Host "`n================================================================================" -ForegroundColor DarkCyan
    Write-Host "  $text" -ForegroundColor Cyan
    Write-Host "================================================================================" -ForegroundColor DarkCyan
}

function Write-Success($text) {
    Write-Host "  [OK] $text" -ForegroundColor Green
}

function Write-Info($text) {
    Write-Host "  [..] $text" -ForegroundColor Yellow
}

function Write-Fail($text) {
    Write-Host "  [FAIL] $text" -ForegroundColor Red
}

# -----------------------------------------------------------------------------
# STEP 1: INFRASTRUCTURE (HDFS 1 NameNode + 3 DataNodes)
# -----------------------------------------------------------------------------
function Start-Infrastructure {
    Write-Header "[BƯỚC 1/4] KHỞI ĐỘNG HẠ TẦNG HDFS (1 NAMENODE + 3 DATANODES)"
    Write-Info "Đang gọi script: 1_infrastructure/start-hdfs.ps1..."
    
    & powershell -ExecutionPolicy Bypass -File "$ProjectRoot\1_infrastructure\start-hdfs.ps1"
    Start-Sleep -Seconds 3

    # Check status
    & powershell -ExecutionPolicy Bypass -File "$ProjectRoot\1_infrastructure\status-hdfs.ps1"
    if ($LASTEXITCODE -ne 0) {
        Write-Fail "HDFS chưa sẵn sàng. Dừng pipeline để tránh lỗi kết nối localhost:9000."
        return $false
    }
    Write-Success "Hạ tầng Big Data HDFS đã sẵn sàng!"
    return $true
}

# -----------------------------------------------------------------------------
# STEP 2: PIPELINE (Tang 1 -> Tang 2 -> Tang 3)
# -----------------------------------------------------------------------------
function Run-PipelineWorkflow {
    Write-Header "[BƯỚC 2/4] THỰC THI PIPELINE 3 TẦNG (TẦNG 1 -> TẦNG 2 -> TẦNG 3)"
    Write-Info "Khởi chạy quy trình Big Data phân tán với Spark 3.5.4..."
    Write-Info "  • Tầng 1: Ingestion CSV thô từ HDFS -> Parquet Snappy (/tang1/jobs)"
    Write-Info "  • Tầng 2: Chuẩn hoá Catalyst, Kiểm toán chất lượng -> (/tang2/jobs)"
    Write-Info "  • Tầng 3: 9 Bảng phân tích Spark SQL & Học máy Spark MLlib -> (/tang3/jobs & data/tang3/)"

    $pythonExe = (Get-Command python -ErrorAction SilentlyContinue).Source
    if (-not $pythonExe) {
        Write-Fail "Không tìm thấy lệnh Python trên hệ thống!"
        return
    }

    & python "$ProjectRoot\2_pipeline\run_pipeline.py"
    if ($LASTEXITCODE -eq 0) {
        Write-Success "Pipeline 3 Tầng hoàn thành xuất sắc! Dữ liệu Tầng 3 đã được cập nhật."
        return $true
    } else {
        Write-Fail "Quá trình thực thi Pipeline gặp lỗi (Exit Code: $LASTEXITCODE)."
        return $false
    }
}

# -----------------------------------------------------------------------------
# STEP 3: BACKEND (FastAPI on Port 8000)
# -----------------------------------------------------------------------------
function Start-BackendService {
    Write-Header "[BƯỚC 3/4] KHỞI ĐỘNG FASTAPI BACKEND SERVICE (PORT 8000)"
    Write-Info "Đang kích hoạt uvicorn phục vụ API dữ liệu Tầng 3..."

    $backendJob = Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$ProjectRoot'; python -m uvicorn 3_backend.app.main:app --host 127.0.0.1 --port 8000 --reload" -PassThru
    Start-Sleep -Seconds 2
    Write-Success "Backend API đang hoạt động tại: http://localhost:8000 (Swagger: http://localhost:8000/docs)"
}

# -----------------------------------------------------------------------------
# STEP 4: FRONTEND (Vite React on Port 5173)
# -----------------------------------------------------------------------------
function Start-FrontendService {
    Write-Header "[BƯỚC 4/4] KHỞI ĐỘNG CLAUDE EDITORIAL DASHBOARD (PORT 5173)"
    Write-Info "Đang khởi chạy Vite React dev server..."

    $frontendJob = Start-Process powershell -ArgumentList "-NoExit", "-Command", "Set-Location '$ProjectRoot\4_frontend'; npm run dev" -PassThru
    Start-Sleep -Seconds 2
    Write-Success "Frontend Dashboard đang chạy tại: http://localhost:5173"
}

# -----------------------------------------------------------------------------
# HDFS INVENTORY (WHAT IS STORED IN THE DATA LAKE)
# -----------------------------------------------------------------------------
function Show-HdfsInventory {
    Write-Header "KIỂM TRA DỮ LIỆU ĐANG LƯU TRONG HDFS"

    $hdfs = "C:\bigdata\hadoop\bin\hdfs.cmd"
    if (-not (Test-Path -LiteralPath $hdfs)) {
        Write-Fail "Không tìm thấy Hadoop CLI tại $hdfs"
        return $false
    }

    $env:JAVA_HOME = "C:\bigdata\java"
    $env:HADOOP_HOME = "C:\bigdata\hadoop"
    $env:HADOOP_CONF_DIR = "C:\bigdata\hadoop\etc\hadoop"

    $paths = @(
        "/raw",
        "/raw/jobs",
        "/raw/uploads",
        "/tang1",
        "/tang2",
        "/tang3",
        "/datasets"
    )

    Write-Host "`n  Dung lượng và số file theo vùng HDFS:" -ForegroundColor Cyan
    foreach ($path in $paths) {
        $result = & $hdfs dfs -count -q -h $path 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "  $result"
        } else {
            Write-Host "  [--] $path chưa tồn tại" -ForegroundColor DarkGray
        }
    }

    Write-Host "`n  Cây thư mục HDFS cấp dữ liệu:" -ForegroundColor Cyan
    & $hdfs dfs -ls -R /raw /tang1 /tang2 /tang3 /datasets 2>&1 |
        Where-Object { $_ -notmatch "No such file or directory" }

    if ($LASTEXITCODE -ne 0) {
        Write-Fail "Không thể đọc inventory HDFS."
        return $false
    }
    Write-Success "Đã đọc inventory HDFS."
    return $true
}

# -----------------------------------------------------------------------------
# CLI ROUTER
# -----------------------------------------------------------------------------
switch ($Step) {
    "hdfs" {
        Start-Infrastructure
    }
    "pipeline" {
        if (-not (Run-PipelineWorkflow)) {
            Write-Fail "Dừng khởi động backend/frontend vì pipeline không hoàn tất."
            break
        }
    }
    "backend" {
        Start-BackendService
    }
    "frontend" {
        Start-FrontendService
    }
    "status" {
        & powershell -ExecutionPolicy Bypass -File "$ProjectRoot\1_infrastructure\status-hdfs.ps1"
    }
    "inventory" {
        Show-HdfsInventory
    }
    "stop" {
        Write-Header "DỪNG TOÀN BỘ TIẾN TRÌNH HẠ TẦNG HDFS"
        & powershell -ExecutionPolicy Bypass -File "$ProjectRoot\1_infrastructure\stop-hdfs.ps1"
    }
    "all" {
        Write-Header "KHỞI CHẠY TOÀN BỘ HỆ THỐNG BIG DATA END-TO-END THEO 4 THƯ MỤC WORKFLOW"
        Write-Host "  [1] Hạ tầng      : 1_infrastructure/ (HDFS 1 NameNode + 3 DataNodes)" -ForegroundColor Gray
        Write-Host "  [2] Pipeline     : 2_pipeline/ (Tầng 1 -> Tầng 2 -> Tầng 3)" -ForegroundColor Gray
        Write-Host "  [3] Backend API  : 3_backend/ (FastAPI Port 8000)" -ForegroundColor Gray
        Write-Host "  [4] Frontend App : 4_frontend/ (React Port 5173)" -ForegroundColor Gray

        if (-not (Start-Infrastructure)) {
            Write-Fail "Không khởi chạy các bước còn lại vì HDFS chưa hoạt động."
            break
        }
        if (-not (Run-PipelineWorkflow)) {
            Write-Fail "Dừng khởi động backend/frontend vì pipeline không hoàn tất."
            break
        }
        Start-BackendService
        Start-FrontendService

        Write-Header "TẤT CẢ DỊCH VỤ ĐÃ ĐƯỢC KÍCH HOẠT THÀNH CÔNG!"
        Write-Host "  • Web Dashboard   : http://localhost:5173" -ForegroundColor Green
        Write-Host "  • Swagger REST API: http://localhost:8000/docs" -ForegroundColor Green
        Write-Host "  • HDFS NameNode   : http://localhost:9870" -ForegroundColor Green
        Write-Host "  • DataNodes WebUI : http://localhost:9864, 9874, 9884" -ForegroundColor Green
        Write-Host ""
    }
}
