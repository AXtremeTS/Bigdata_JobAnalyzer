# scripts/status-hdfs.ps1
# Checks HDFS cluster health & active DataNodes count

$BIGDATA_ROOT = "C:\bigdata"
$HADOOP       = "$BIGDATA_ROOT\hadoop"
$JAVA_HOME    = "$BIGDATA_ROOT\java"

$env:JAVA_HOME = $JAVA_HOME
$env:HADOOP_HOME = $HADOOP
$env:HADOOP_CONF_DIR = "$HADOOP\etc\hadoop"
$env:Path = "$JAVA_HOME\bin;$HADOOP\bin;$HADOOP\sbin;$env:Path"

Write-Host "=========================================================="
Write-Host "  HDFS CLUSTER STATUS (1 NAMENODE + 3 DATANODES)"
Write-Host "=========================================================="

Write-Host "`n[1] Checking Active Java Processes:"
Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -match "namenode\.NameNode|datanode\.DataNode"
} | Select-Object ProcessId, CommandLine | ForEach-Object {
    $role = if ($_.CommandLine -match "NameNode") { "NameNode" } else { "DataNode" }
    Write-Host "  - PID $($_.ProcessId): $role"
}

Write-Host "`n[2] HDFS Admin Report:"
$report = & "$HADOOP\bin\hdfs.cmd" dfsadmin -report 2>&1
$report
if ($LASTEXITCODE -ne 0 -or -not ($report -match "Live datanodes")) {
    Write-Host "`n[FAIL] HDFS is not reachable or has no live DataNodes." -ForegroundColor Red
    exit 1
}

Write-Host "`n[3] HDFS File System Check (fsck):"
& "$HADOOP\bin\hdfs.cmd" fsck / -files -blocks -locations
if ($LASTEXITCODE -ne 0) {
    Write-Host "`n[FAIL] HDFS fsck failed." -ForegroundColor Red
    exit 1
}

Write-Host "`n[OK] HDFS cluster is healthy." -ForegroundColor Green
exit 0
