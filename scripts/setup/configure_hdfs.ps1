# scripts/setup/configure_hdfs.ps1
# Configure Hadoop HDFS for 1 NameNode + 3 DataNodes on Windows Native
# Run AFTER install_bigdata.ps1

$ErrorActionPreference = 'Stop'

$BIGDATA_ROOT = "C:\bigdata"
$HADOOP_DIR   = "$BIGDATA_ROOT\hadoop"
$CONF_DIR     = "$HADOOP_DIR\etc\hadoop"
$DATA_ROOT    = "$BIGDATA_ROOT\hdfs-data"
$LOGS_DIR     = "$BIGDATA_ROOT\logs"
$JAVA_HOME    = "$BIGDATA_ROOT\java"

Write-Host "=========================================================="
Write-Host "  CONFIGURING HDFS: 1 NAMENODE + 3 DATANODES (WINDOWS)"
Write-Host "=========================================================="

if (-not (Test-Path "$HADOOP_DIR\bin\hdfs.cmd")) {
    Write-Error "Hadoop not found at $HADOOP_DIR! Run install_bigdata.ps1 first."
    exit 1
}

# --- 1. core-site.xml (Shared across NameNode & all DataNodes) ---
Write-Host "`n[1/6] Writing core-site.xml..."
$coreSiteContent = @"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://localhost:9000</value>
    </property>
    <property>
        <name>hadoop.tmp.dir</name>
        <value>C:/bigdata/hdfs-data/tmp</value>
    </property>
    <property>
        <name>dfs.webhdfs.enabled</name>
        <value>true</value>
    </property>
</configuration>
"@
Set-Content -Path "$CONF_DIR\core-site.xml" -Value $coreSiteContent -Encoding UTF8

# --- 2. hdfs-site.xml for NameNode ---
Write-Host "[2/6] Writing NameNode hdfs-site.xml..."
$hdfsSiteContent = @"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <!-- Cluster Replication Factor = 3 (across 3 DataNodes) -->
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.namenode.name.dir</name>
        <value>file:///C:/bigdata/hdfs-data/namenode</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>0.0.0.0:9870</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
    <property>
        <name>dfs.blocksize</name>
        <value>134217728</value>
    </property>
    <property>
        <name>dfs.namenode.datanode.registration.ip-hostname-check</name>
        <value>false</value>
    </property>
</configuration>
"@
Set-Content -Path "$CONF_DIR\hdfs-site.xml" -Value $hdfsSiteContent -Encoding UTF8

# --- 3. hadoop-env.cmd ---
Write-Host "[3/6] Writing hadoop-env.cmd..."
$hadoopEnvContent = @"
@rem Set JAVA_HOME and HADOOP paths (Windows Native)
set JAVA_HOME=$JAVA_HOME
set HADOOP_HOME=$HADOOP_DIR
"@
Set-Content -Path "$CONF_DIR\hadoop-env.cmd" -Value $hadoopEnvContent -Encoding UTF8

# --- 4. Per-DataNode Configurations (3 DataNodes on distinct ports) ---
Write-Host "[4/6] Creating individual DataNode configs (dn1, dn2, dn3)..."
$datanodes = @(
    @{ id=1; httpPort=9864; httpsPort=9865; dataPort=9866; ipcPort=9867; dir="C:/bigdata/hdfs-data/datanode1" },
    @{ id=2; httpPort=9874; httpsPort=9875; dataPort=9876; ipcPort=9877; dir="C:/bigdata/hdfs-data/datanode2" },
    @{ id=3; httpPort=9884; httpsPort=9885; dataPort=9886; ipcPort=9887; dir="C:/bigdata/hdfs-data/datanode3" }
)

foreach ($dn in $datanodes) {
    $dnConfDir = "$DATA_ROOT\dn$($dn.id)-conf"
    if (-not (Test-Path $dnConfDir)) {
        New-Item -ItemType Directory -Path $dnConfDir -Force | Out-Null
    }

    # Copy core-site.xml
    Copy-Item "$CONF_DIR\core-site.xml" "$dnConfDir\core-site.xml" -Force
    Copy-Item "$CONF_DIR\hadoop-env.cmd" "$dnConfDir\hadoop-env.cmd" -Force

    # Write DataNode-specific hdfs-site.xml
    $dnHdfsSite = @"
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>3</value>
    </property>
    <property>
        <name>dfs.datanode.data.dir</name>
        <value>file:///$($dn.dir)</value>
    </property>
    <property>
        <name>dfs.datanode.http.address</name>
        <value>0.0.0.0:$($dn.httpPort)</value>
    </property>
    <property>
        <name>dfs.datanode.https.address</name>
        <value>0.0.0.0:$($dn.httpsPort)</value>
    </property>
    <property>
        <name>dfs.datanode.address</name>
        <value>0.0.0.0:$($dn.dataPort)</value>
    </property>
    <property>
        <name>dfs.datanode.ipc.address</name>
        <value>0.0.0.0:$($dn.ipcPort)</value>
    </property>
    <property>
        <name>dfs.namenode.http-address</name>
        <value>localhost:9870</value>
    </property>
    <property>
        <name>dfs.permissions.enabled</name>
        <value>false</value>
    </property>
</configuration>
"@
    Set-Content -Path "$dnConfDir\hdfs-site.xml" -Value $dnHdfsSite -Encoding UTF8
    Write-Host "  DataNode $($dn.id): Config=$dnConfDir, Storage=$($dn.dir), HTTP=:$($dn.httpPort), Data=:$($dn.dataPort), IPC=:$($dn.ipcPort)"
}

# --- 5. Format NameNode (Safe check: only if not already formatted) ---
Write-Host "`n[5/6] Checking NameNode formatting..."
$env:JAVA_HOME = $JAVA_HOME
$env:HADOOP_HOME = $HADOOP_DIR
$env:HADOOP_CONF_DIR = $CONF_DIR
$env:Path = "$JAVA_HOME\bin;$HADOOP_DIR\bin;$HADOOP_DIR\sbin;$env:Path"

$nnVersionFile = "$DATA_ROOT\namenode\current\VERSION"
if (-not (Test-Path $nnVersionFile)) {
    Write-Host "  Formatting NameNode for the first time..."
    & "$HADOOP_DIR\bin\hdfs.cmd" namenode -format -force -nonInteractive
    Write-Host "  [OK] NameNode formatted successfully."
} else {
    Write-Host "  [SKIP] NameNode already formatted (found $nnVersionFile)"
}

# --- 6. Generate Cluster Management Scripts ---
Write-Host "`n[6/6] Generating cluster management scripts..."

# scripts/start-hdfs.ps1
$startScript = @'
# scripts/start-hdfs.ps1
# Starts 1 NameNode + 3 DataNodes on Windows Native

$BIGDATA_ROOT = "C:\bigdata"
$HADOOP       = "$BIGDATA_ROOT\hadoop"
$JAVA_HOME    = "$BIGDATA_ROOT\java"
$LOGS         = "$BIGDATA_ROOT\logs"

$env:JAVA_HOME = $JAVA_HOME
$env:HADOOP_HOME = $HADOOP
$env:Path = "$JAVA_HOME\bin;$HADOOP\bin;$HADOOP\sbin;$env:Path"

Write-Host "=========================================================="
Write-Host "  STARTING HDFS CLUSTER: 1 NAMENODE + 3 DATANODES"
Write-Host "=========================================================="

if (-not (Test-Path $LOGS)) { New-Item -ItemType Directory -Path $LOGS -Force | Out-Null }

# 1. Start NameNode
Write-Host "`n[1/4] Starting NameNode..."
$nnConf = "$HADOOP\etc\hadoop"
$nnCmd = "set JAVA_HOME=$JAVA_HOME && set HADOOP_HOME=$HADOOP && set HADOOP_CONF_DIR=$nnConf && `"$HADOOP\bin\hdfs.cmd`" namenode"
Start-Process -FilePath "cmd.exe" -ArgumentList "/k title NameNode && $nnCmd" -WindowStyle Minimized

Write-Host "  Waiting for NameNode to initialize (6 seconds)..."
Start-Sleep -Seconds 6

# 2. Start DataNodes 1, 2, 3
$datanodes = @(
    @{ id=1; conf="C:\bigdata\hdfs-data\dn1-conf"; port=9864 },
    @{ id=2; conf="C:\bigdata\hdfs-data\dn2-conf"; port=9874 },
    @{ id=3; conf="C:\bigdata\hdfs-data\dn3-conf"; port=9884 }
)

foreach ($dn in $datanodes) {
    Write-Host "[$(1 + $dn.id)/4] Starting DataNode $($dn.id) (WebUI: http://localhost:$($dn.port))..."
    $dnCmd = "set JAVA_HOME=$JAVA_HOME && set HADOOP_HOME=$HADOOP && set HADOOP_CONF_DIR=$($dn.conf) && `"$HADOOP\bin\hdfs.cmd`" datanode"
    Start-Process -FilePath "cmd.exe" -ArgumentList "/k title DataNode_$($dn.id) && $dnCmd" -WindowStyle Minimized
    Start-Sleep -Seconds 3
}

Write-Host "`nWaiting 10 seconds for DataNodes to register with NameNode..."
Start-Sleep -Seconds 10

Write-Host "`n=== HDFS CLUSTER SERVICES ONLINE ==="
Write-Host "  NameNode Web UI:   http://localhost:9870"
Write-Host "  DataNode 1 Web UI: http://localhost:9864"
Write-Host "  DataNode 2 Web UI: http://localhost:9874"
Write-Host "  DataNode 3 Web UI: http://localhost:9884"
Write-Host "`nRunning cluster health check (hdfs dfsadmin -report)..."
& "$HADOOP\bin\hdfs.cmd" dfsadmin -report | Select-Object -First 18
'@
Set-Content -Path "$PSScriptRoot\..\start-hdfs.ps1" -Value $startScript -Encoding UTF8

# scripts/stop-hdfs.ps1
$stopScript = @'
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
'@
Set-Content -Path "$PSScriptRoot\..\stop-hdfs.ps1" -Value $stopScript -Encoding UTF8

# scripts/status-hdfs.ps1
$statusScript = @'
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
& "$HADOOP\bin\hdfs.cmd" dfsadmin -report

Write-Host "`n[3] HDFS File System Check (fsck):"
& "$HADOOP\bin\hdfs.cmd" fsck / -files -blocks -locations
'@
Set-Content -Path "$PSScriptRoot\..\status-hdfs.ps1" -Value $statusScript -Encoding UTF8

Write-Host "`n[SUCCESS] HDFS 1 NameNode + 3 DataNodes configuration ready!"
Write-Host "Scripts created:"
Write-Host "  - scripts/start-hdfs.ps1"
Write-Host "  - scripts/stop-hdfs.ps1"
Write-Host "  - scripts/status-hdfs.ps1"
