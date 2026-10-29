# scripts/start-hdfs.ps1
# Starts 1 NameNode + 3 DataNodes on Windows Native (with verified --config isolation)

$BIGDATA_ROOT = "C:\bigdata"
$HADOOP       = "$BIGDATA_ROOT\hadoop"
$JAVA_HOME    = "$BIGDATA_ROOT\java"
$LOGS         = "$BIGDATA_ROOT\logs"

$env:JAVA_HOME = $JAVA_HOME
$env:HADOOP_HOME = $HADOOP
$env:Path = "$JAVA_HOME\bin;$HADOOP\bin;$HADOOP\sbin;$env:Path"

Write-Host "=========================================================="
Write-Host "  STARTING HDFS CLUSTER: 1 NAMENODE + 3 DATANODES (WINDOWS)"
Write-Host "=========================================================="

if (-not (Test-Path $LOGS)) { New-Item -ItemType Directory -Path $LOGS -Force | Out-Null }

# 1. Start NameNode (if not already running)
$nnRunning = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "namenode\.NameNode" }
if (-not $nnRunning) {
    Write-Host "`n[1/4] Starting NameNode..."
    $nnCmd = "cd /d C:\bigdata && set JAVA_HOME=$JAVA_HOME && set HADOOP_HOME=$HADOOP && set HADOOP_CONF_DIR=$HADOOP\etc\hadoop && `"$HADOOP\bin\hdfs.cmd`" namenode"
    Start-Process -FilePath "cmd.exe" -ArgumentList "/k title NameNode && $nnCmd" -WorkingDirectory "C:\bigdata" -WindowStyle Minimized
    Write-Host "  Waiting for NameNode to initialize (6 seconds)..."
    Start-Sleep -Seconds 6
} else {
    Write-Host "`n[1/4] NameNode already running (PID: $($nnRunning.ProcessId))"
}

# 2. Start 3 DataNodes with isolated configs
$datanodes = @(
    @{ id=1; conf="C:\bigdata\hdfs-data\dn1-conf"; port=9864 },
    @{ id=2; conf="C:\bigdata\hdfs-data\dn2-conf"; port=9874 },
    @{ id=3; conf="C:\bigdata\hdfs-data\dn3-conf"; port=9884 }
)

foreach ($dn in $datanodes) {
    $dnRunning = Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match "dn$($dn.id)-conf" -or ($_.CommandLine -match "datanode\.DataNode" -and $_.CommandLine -match ":98$($dn.id)6") }
    if (-not $dnRunning) {
        Write-Host "[$(1 + $dn.id)/4] Starting DataNode $($dn.id) (WebUI: http://localhost:$($dn.port))..."
        $dnCmd = "cd /d C:\bigdata && set JAVA_HOME=$JAVA_HOME && set HADOOP_HOME=$HADOOP && `"$HADOOP\bin\hdfs.cmd`" --config `"$($dn.conf)`" datanode"
        Start-Process -FilePath "cmd.exe" -ArgumentList "/k title DataNode_$($dn.id) && $dnCmd" -WorkingDirectory "C:\bigdata" -WindowStyle Minimized
        Start-Sleep -Seconds 3
    } else {
        Write-Host "[$(1 + $dn.id)/4] DataNode $($dn.id) already running."
    }
}

Write-Host "`n=== HDFS CLUSTER SERVICES ONLINE ==="
Write-Host "  NameNode Web UI:   http://localhost:9870"
Write-Host "  DataNode 1 Web UI: http://localhost:9864"
Write-Host "  DataNode 2 Web UI: http://localhost:9874"
Write-Host "  DataNode 3 Web UI: http://localhost:9884"
Write-Host "`nRunning cluster health check (hdfs dfsadmin -report)..."
Set-Location "C:\bigdata"
& "$HADOOP\bin\hdfs.cmd" dfsadmin -report | Select-Object -First 25
