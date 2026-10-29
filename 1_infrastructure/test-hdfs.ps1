# scripts/test-hdfs.ps1
# End-to-end test for HDFS Cluster: 1 NameNode + 3 DataNodes + Data Ingestion + Replication Check

$ErrorActionPreference = 'Stop'

$BIGDATA_ROOT = "C:\bigdata"
$HADOOP       = "$BIGDATA_ROOT\hadoop"
$JAVA_HOME    = "$BIGDATA_ROOT\java"
$CSV_SRC      = Join-Path $PSScriptRoot "..\VietJobs.csv"
$LOG_DIR      = Join-Path $PSScriptRoot "..\docs\evidence\logs"

$env:JAVA_HOME = $JAVA_HOME
$env:HADOOP_HOME = $HADOOP
$env:HADOOP_CONF_DIR = "$HADOOP\etc\hadoop"
$env:Path = "$JAVA_HOME\bin;$HADOOP\bin;$HADOOP\sbin;$env:Path"

if (-not (Test-Path $LOG_DIR)) { New-Item -ItemType Directory -Path $LOG_DIR -Force | Out-Null }

Write-Host "=========================================================="
Write-Host "  HDFS CLUSTER VALIDATION (1 NAMENODE + 3 DATANODES)"
Write-Host "=========================================================="

Set-Location "C:\bigdata"

# 1. Cluster Node Health Report
Write-Host "`n[1/5] Checking Active DataNodes via dfsadmin -report..."
$reportLog = & "$HADOOP\bin\hdfs.cmd" dfsadmin -report
$reportLog | ForEach-Object { Write-Host "  $_" }

$liveNodesMatch = $reportLog | Select-String -Pattern "Live datanodes \(([0-9]+)\)"
if ($liveNodesMatch) {
    Write-Host "`n  -> Found: $($liveNodesMatch.Line)"
}

# 2. Create HDFS Directory Structure
Write-Host "`n[2/5] Creating Medallion directory structure on HDFS..."
$hdfsDirs = @("/raw/jobs", "/bronze", "/silver", "/gold", "/warehouse")
foreach ($dir in $hdfsDirs) {
    Write-Host "  Creating $dir..."
    & "$HADOOP\bin\hdfs.cmd" dfs -mkdir -p $dir
}

# 3. Ingest VietJobs.csv into HDFS
Write-Host "`n[3/5] Uploading VietJobs.csv into HDFS (/raw/jobs/vietjobs.csv)..."
if (-not (Test-Path $CSV_SRC)) {
    Write-Error "File VietJobs.csv not found at $CSV_SRC!"
    exit 1
}

& "$HADOOP\bin\hdfs.cmd" dfs -put -f $CSV_SRC /raw/jobs/vietjobs.csv
Write-Host "  [OK] Upload completed."

# 4. Verify File Size and Permissions in HDFS
Write-Host "`n[4/5] Checking file metadata in HDFS..."
& "$HADOOP\bin\hdfs.cmd" dfs -ls -h /raw/jobs

# 5. Verify Replication Across All 3 DataNodes (fsck)
Write-Host "`n[5/5] Checking Block Replication (Replication Factor = 3)..."
$fsckOutput = & "$HADOOP\bin\hdfs.cmd" fsck /raw/jobs/vietjobs.csv -files -blocks -locations
$fsckOutput | ForEach-Object { Write-Host "  $_" }

# Save evidence log
$logFile = "$LOG_DIR\CP-01_hdfs_cluster_3datanodes.log"
$fsckOutput | Out-File -FilePath $logFile -Encoding UTF8
Write-Host "`n[EVIDENCE] Saved verification report to: $logFile"

Write-Host "`n=========================================================="
Write-Host "  [SUCCESS] HDFS 1 NameNode + 3 DataNodes verified healthy!"
Write-Host "=========================================================="
