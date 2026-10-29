# scripts/setup/install_bigdata.ps1
# Download and install Hadoop 3.3.6 + Spark 3.5.8 for Windows Native
# Configured for 1 NameNode + 3 DataNodes requirement

$ErrorActionPreference = 'Stop'

$BIGDATA_ROOT  = "C:\bigdata"
$HADOOP_VER    = "3.3.6"
$SPARK_VER     = "3.5.8"
$HADOOP_DIR    = "$BIGDATA_ROOT\hadoop"
$SPARK_DIR     = "$BIGDATA_ROOT\spark"
$DATA_ROOT     = "$BIGDATA_ROOT\hdfs-data"
$WINUTILS_DIR  = "$BIGDATA_ROOT\winutils"
$JAVA_JUNCTION = "$BIGDATA_ROOT\java"

Write-Host "=========================================================="
Write-Host "  JOBMARKET INTELLIGENCE — INSTALL HADOOP & SPARK (WINDOWS)"
Write-Host "=========================================================="

# 1. OpenJDK 17 Setup & Junction
Write-Host "`n[1/6] Checking OpenJDK 17..."
$jdk17Path = Get-ChildItem "C:\Program Files\Microsoft" -Filter "jdk-17*" -Directory -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $jdk17Path) {
    $jdk17Path = Get-ChildItem "C:\Program Files\Eclipse Adoptium" -Filter "jdk-17*" -Directory -ErrorAction SilentlyContinue | Select-Object -First 1
}
if (-not $jdk17Path) {
    Write-Error "OpenJDK 17 not found! Run: winget install Microsoft.OpenJDK.17"
    exit 1
}

if (-not (Test-Path $JAVA_JUNCTION)) {
    New-Item -ItemType Junction -Path $JAVA_JUNCTION -Target $jdk17Path.FullName -Force | Out-Null
    Write-Host "  Created directory junction: $JAVA_JUNCTION -> $($jdk17Path.FullName)"
} else {
    Write-Host "  Directory junction exists: $JAVA_JUNCTION -> $($jdk17Path.FullName)"
}
$JAVA_HOME = $JAVA_JUNCTION
Write-Host "  JAVA_HOME (no spaces) = $JAVA_HOME"

# 2. Directory Structure for 1 NameNode + 3 DataNodes
Write-Host "`n[2/6] Creating directory layout..."
$dirs = @(
    $BIGDATA_ROOT,
    $DATA_ROOT,
    "$DATA_ROOT\namenode",
    "$DATA_ROOT\datanode1",
    "$DATA_ROOT\datanode2",
    "$DATA_ROOT\datanode3",
    "$DATA_ROOT\tmp",
    "$DATA_ROOT\dn1-conf",
    "$DATA_ROOT\dn2-conf",
    "$DATA_ROOT\dn3-conf",
    "$BIGDATA_ROOT\logs",
    $WINUTILS_DIR
)
foreach ($d in $dirs) {
    if (-not (Test-Path $d)) {
        New-Item -ItemType Directory -Path $d -Force | Out-Null
        Write-Host "  Created: $d"
    }
}

# 3. Download & Extract Hadoop 3.3.6 (using high-speed dlcdn mirror)
Write-Host "`n[3/6] Setting up Hadoop $HADOOP_VER..."
$hadoopTar = "$BIGDATA_ROOT\hadoop-$HADOOP_VER.tar.gz"
if (-not (Test-Path "$HADOOP_DIR\bin\hadoop.cmd")) {
    if (-not (Test-Path $hadoopTar)) {
        Write-Host "  Downloading Hadoop $HADOOP_VER (dlcdn.apache.org)..."
        $hadoopUrl = "https://dlcdn.apache.org/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz"
        & curl.exe -L -C - --retry 3 --progress-bar -o $hadoopTar $hadoopUrl
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path $hadoopTar)) {
            Write-Host "  Falling back to archive.apache.org..."
            $hadoopUrl = "https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VER/hadoop-$HADOOP_VER.tar.gz"
            & curl.exe -L -C - --retry 3 --progress-bar -o $hadoopTar $hadoopUrl
        }
    }
    Write-Host "  Extracting Hadoop archive..."
    & tar.exe -xzf $hadoopTar -C $BIGDATA_ROOT
    if (Test-Path "$BIGDATA_ROOT\hadoop-$HADOOP_VER") {
        if (Test-Path $HADOOP_DIR) { Remove-Item $HADOOP_DIR -Recurse -Force }
        Rename-Item "$BIGDATA_ROOT\hadoop-$HADOOP_VER" $HADOOP_DIR
    }
    Write-Host "  Hadoop extracted to: $HADOOP_DIR"
} else {
    Write-Host "  [SKIP] Hadoop already installed at $HADOOP_DIR"
}

# 4. Download & Extract Spark 3.5.8 (using high-speed dlcdn mirror)
Write-Host "`n[4/6] Setting up Spark $SPARK_VER..."
$sparkTgz = "$BIGDATA_ROOT\spark-$SPARK_VER-bin-hadoop3.tgz"
if (-not (Test-Path "$SPARK_DIR\bin\spark-submit.cmd")) {
    if (-not (Test-Path $sparkTgz)) {
        Write-Host "  Downloading Spark $SPARK_VER (dlcdn.apache.org)..."
        $sparkUrl = "https://dlcdn.apache.org/spark/spark-$SPARK_VER/spark-$SPARK_VER-bin-hadoop3.tgz"
        & curl.exe -L -C - --retry 3 --progress-bar -o $sparkTgz $sparkUrl
        if ($LASTEXITCODE -ne 0 -or -not (Test-Path $sparkTgz)) {
            Write-Host "  Falling back to archive.apache.org..."
            $sparkUrl = "https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz"
            & curl.exe -L -C - --retry 3 --progress-bar -o $sparkTgz $sparkUrl
        }
    }
    Write-Host "  Extracting Spark archive..."
    & tar.exe -xzf $sparkTgz -C $BIGDATA_ROOT
    $extractedSparkDir = Get-ChildItem -Path $BIGDATA_ROOT -Filter "spark-3*" -Directory | Where-Object { $_.Name -ne "spark" } | Select-Object -First 1
    if ($extractedSparkDir) {
        if (Test-Path $SPARK_DIR) { Remove-Item $SPARK_DIR -Recurse -Force }
        Rename-Item $extractedSparkDir.FullName $SPARK_DIR
    }
    Write-Host "  Spark extracted to: $SPARK_DIR"
} else {
    Write-Host "  [SKIP] Spark already installed at $SPARK_DIR"
}

# 5. Windows Native winutils & hadoop.dll
Write-Host "`n[5/6] Setting up Windows native binaries (winutils & hadoop.dll)..."
$winutilsUrl = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/winutils.exe"
$hadoopDllUrl = "https://raw.githubusercontent.com/cdarlint/winutils/master/hadoop-3.3.6/bin/hadoop.dll"

if (-not (Test-Path "$WINUTILS_DIR\winutils.exe")) {
    Write-Host "  Downloading winutils.exe..."
    & curl.exe -L -s -o "$WINUTILS_DIR\winutils.exe" $winutilsUrl
}
if (-not (Test-Path "$WINUTILS_DIR\hadoop.dll")) {
    Write-Host "  Downloading hadoop.dll..."
    & curl.exe -L -s -o "$WINUTILS_DIR\hadoop.dll" $hadoopDllUrl
}

Copy-Item "$WINUTILS_DIR\winutils.exe" "$HADOOP_DIR\bin\winutils.exe" -Force
Copy-Item "$WINUTILS_DIR\hadoop.dll" "$HADOOP_DIR\bin\hadoop.dll" -Force
if (Test-Path "$SPARK_DIR\bin") {
    Copy-Item "$WINUTILS_DIR\winutils.exe" "$SPARK_DIR\bin\winutils.exe" -Force
    Copy-Item "$WINUTILS_DIR\hadoop.dll" "$SPARK_DIR\bin\hadoop.dll" -Force
}
Write-Host "  Placed winutils.exe and hadoop.dll into Hadoop and Spark bin"

# 6. Configure Environment Variables
Write-Host "`n[6/6] Configuring environment variables..."
[System.Environment]::SetEnvironmentVariable("JAVA_HOME", $JAVA_HOME, "User")
[System.Environment]::SetEnvironmentVariable("HADOOP_HOME", $HADOOP_DIR, "User")
[System.Environment]::SetEnvironmentVariable("SPARK_HOME", $SPARK_DIR, "User")
[System.Environment]::SetEnvironmentVariable("HADOOP_CONF_DIR", "$HADOOP_DIR\etc\hadoop", "User")

$currentPath = [System.Environment]::GetEnvironmentVariable("Path", "User")
$pathsToAdd = @(
    "$JAVA_HOME\bin",
    "$HADOOP_DIR\bin",
    "$HADOOP_DIR\sbin",
    "$SPARK_DIR\bin"
)
foreach ($p in $pathsToAdd) {
    if ($currentPath -notlike "*$p*") {
        $currentPath = "$p;$currentPath"
    }
}
[System.Environment]::SetEnvironmentVariable("Path", $currentPath, "User")

# Apply to current session
$env:JAVA_HOME = $JAVA_HOME
$env:HADOOP_HOME = $HADOOP_DIR
$env:SPARK_HOME = $SPARK_DIR
$env:HADOOP_CONF_DIR = "$HADOOP_DIR\etc\hadoop"
$env:Path = "$JAVA_HOME\bin;$HADOOP_DIR\bin;$HADOOP_DIR\sbin;$SPARK_DIR\bin;$env:Path"

Write-Host "`n=== Installation Verification ==="
Write-Host "Java:"
& "$JAVA_HOME\bin\java.exe" -version 2>&1 | Select-Object -First 2 | ForEach-Object { Write-Host "  $_" }

Write-Host "Winutils:"
& "$HADOOP_DIR\bin\winutils.exe" systeminfo 2>&1 | Select-Object -First 2 | ForEach-Object { Write-Host "  $_" }

Write-Host "Hadoop:"
& "$HADOOP_DIR\bin\hadoop.cmd" version 2>&1 | Select-Object -First 2 | ForEach-Object { Write-Host "  $_" }

Write-Host "`n[SUCCESS] Hadoop and Spark installation complete!"
Write-Host "`n[7/7] Installing the pinned Airflow environment..."
& powershell -ExecutionPolicy Bypass -File "$PSScriptRoot\install_airflow.ps1"
if ($LASTEXITCODE -ne 0) {
    throw "Airflow installation failed. HDFS/Spark installation completed, but orchestration is not ready."
}
