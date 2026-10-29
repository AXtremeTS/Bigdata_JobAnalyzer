# scripts/stop-hdfs.ps1
# Gracefully stops all NameNode and DataNode processes

Write-Host "=========================================================="
Write-Host "  STOPPING HDFS CLUSTER"
Write-Host "=========================================================="

Get-CimInstance Win32_Process | Where-Object {
    $_.CommandLine -match "org.apache.hadoop.hdfs.server.(namenode.NameNode|datanode.DataNode)" -or
    $_.CommandLine -match "hdfs.cmd.*(namenode|datanode)"
} | ForEach-Object {
    Write-Host "  Terminating Process PID $($_.ProcessId): $($_.Name)"
    Stop-Process -Id $_.ProcessId -Force -ErrorAction SilentlyContinue
}

# Also close CMD windows titled NameNode or DataNode
Get-Process cmd -ErrorAction SilentlyContinue | Where-Object {
    $_.MainWindowTitle -match "NameNode|DataNode"
} | ForEach-Object {
    Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
}

Write-Host "`n[OK] All HDFS services stopped."
