function Read-Config($filePath) {
    $config = @{}
    $section = ""

    foreach ($line in Get-Content $filePath) {
        $line = $line.Trim()
        if ($line -match "^\[(.+)\]$") {
            $section = $matches[1]
            $config[$section] = @{}
        } elseif ($line -match "^(.*?)=(.*)$") {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            $config[$section][$key] = $value
        }
    }
    return $config
}

function Get-SqlServerSamples($sqlConn, $sqlMeta, $outputDir) {
    $dbName = $sqlMeta.database
    $schema = $sqlMeta.schema
    $connStr = "Server=$($sqlConn.host),$($sqlConn.port);Database=$dbName;User ID=$($sqlConn.username);Password=$($sqlConn.password);"

    $tableQuery = @"
SELECT t.name
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = '$schema';
"@

    $tables = Invoke-Sqlcmd -ConnectionString $connStr -Query $tableQuery

    foreach ($t in $tables) {
        $table = $t.name
        $query = "SELECT TOP 20 * FROM [$schema].[$table];"

        try {
            $data = Invoke-Sqlcmd -ConnectionString $connStr -Query $query
            $filePath = Join-Path $outputDir "SQLServer_${schema}_$table.csv"
            $data | Export-Csv -Path $filePath -NoTypeInformation -Encoding UTF8
            Write-Host "Exported SQL Server sample: $table" -ForegroundColor Cyan
        } catch {
            Write-Warning "Failed to export table $table from SQL Server: $_"
        }
    }
}

function Get-PostgresSamples($pgConn, $pgMeta, $outputDir) {
    $dbName = $pgMeta.database
    $schema = $pgMeta.schema
    $env:PGPASSWORD = $pgConn.password

    $tableQuery = @"
SELECT table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema = '$schema';
"@

    $tableList = & psql -h $pgConn.host -p $pgConn.port -U $pgConn.username -d $dbName -t -A -c "$tableQuery"
    if (-not $tableList) { return }

    $tables = $tableList -split "`n" | Where-Object { $_ -ne "" }

    foreach ($table in $tables) {
        $qualified = "$schema.`"$table`""
        $sampleQuery = "SELECT * FROM $qualified LIMIT 20;"
        try {
            $outFile = Join-Path $outputDir "Postgres_${schema}_$table.csv"
            & psql -h $pgConn.host -p $pgConn.port -U $pgConn.username -d $dbName -F ',' --csv -c "$sampleQuery" > $outFile
            Write-Host "Exported PostgreSQL sample: $table" -ForegroundColor Green
        } catch {
            Write-Warning "Failed to export table $table from PostgreSQL: $_"
        }
    }

    Remove-Item Env:\PGPASSWORD
}

# ======== MAIN SCRIPT ========

$configPath = ".\db_cred.conf"
$metaPath = ".\db_metadata.conf"
$outputDir = ".\samples_records"

# Create output folder
if (-not (Test-Path $outputDir)) {
    New-Item -ItemType Directory -Path $outputDir | Out-Null
}

# Read config
$connConfig = Read-Config $configPath
$metaConfig = Read-Config $metaPath

if ($connConfig.ContainsKey("SQLServer") -and $metaConfig.ContainsKey("SQLServer")) {
    Get-SqlServerSamples $connConfig["SQLServer"] $metaConfig["SQLServer"] $outputDir
}
if ($connConfig.ContainsKey("PostgreSQL") -and $metaConfig.ContainsKey("PostgreSQL")) {
    Get-PostgresSamples $connConfig["PostgreSQL"] $metaConfig["PostgreSQL"] $outputDir
}

Write-Host "`nSample data exported to: $outputDir" -ForegroundColor Yellow
