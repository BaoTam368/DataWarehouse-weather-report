# ============================== 
# File: DailyETL.ps1 
# Java ETL + Logging + Auto Clean Logs (30 days)
# ==============================

# ============= CONFIG =============
# Set working directory to project folder
Set-Location "D:\DW\DataWarehouse"

$JAVA = "C:\Program Files\Java\jdk-21\bin\java.exe"

# FAT JAR (built by shade plugin)
$JAR = "D:\DW\DataWarehouse\target\DataWarehouse-0.0.1-SNAPSHOT.jar"

# Log folder INSIDE the project (NOT inside target)
$LOG_DIR = "D:\DW\DataWarehouse\logs"

# Filename: daily_YYYY-MM-DD_HH-mm-ss.log
$DATE = Get-Date -Format "yyyy-MM-dd_HH-mm-ss"
$LOG_FILE = "$LOG_DIR\daily_$DATE.log"

# ============= CREATE LOG FOLDER IF NEEDED =============
if (!(Test-Path $LOG_DIR)) {
    New-Item -ItemType Directory -Path $LOG_DIR | Out-Null
}

# ============= START LOG =============
"======================================" | Out-File $LOG_FILE -Append
"START ETL: $DATE"                     | Out-File $LOG_FILE -Append
"======================================" | Out-File $LOG_FILE -Append

# ============= RUN JAVA PROGRAM =============
try {
    & "$JAVA" -jar "$JAR" >> $LOG_FILE 2>&1
    "SUCCESS: Java ETL completed at $(Get-Date)" | Out-File $LOG_FILE -Append
}
catch {
    "ERROR: Java ETL failed at $(Get-Date)" | Out-File $LOG_FILE -Append
    $_ | Out-File $LOG_FILE -Append
}

# ============= AUTO CLEAN LOGS OLDER THAN 30 DAYS =============
$limit = (Get-Date).AddDays(-30)
Get-ChildItem -Path $LOG_DIR -File -Filter "*.log" |
    Where-Object { $_.LastWriteTime -lt $limit } |
    Remove-Item -Force

"LOG CLEANUP: Removed logs older than 30 days" | Out-File $LOG_FILE -Append

# ============= END LOG =============
"--------------------------------------" | Out-File $LOG_FILE -Append
