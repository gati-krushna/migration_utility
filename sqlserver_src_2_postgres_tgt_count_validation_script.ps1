function Read-Config($filePath) {
    Write-Host "Reading config from: $filePath"
    $config = @{}
    $section = ""

    foreach ($line in Get-Content $filePath) {
        $line = $line.Trim()
        if ($line -match "^\[(.+)\]$") {
            $section = $matches[1]
            Write-Host "Found section: $section"
            $config[$section] = @{}
        } elseif ($line -match "^(.*?)=(.*)$") {
            $key = $matches[1].Trim()
            $value = $matches[2].Trim()
            Write-Host "Key: $key, Value: $value under [$section]"
            $config[$section][$key] = $value
        }
    }
    return $config
}

function Get-SqlServerTableCounts($sqlConn, $sqlMeta) {
    $dbName = $sqlMeta.database
    $schema = $sqlMeta.schema
    Write-Host "`nSQL Server - Database: $dbName, Schema: $schema" -ForegroundColor Cyan

    $connStr = "Server=$($sqlConn.host),$($sqlConn.port);Database=$dbName;User ID=$($sqlConn.username);Password=$($sqlConn.password);"

    $schemaCondition = "AND s.name = '$schema'"

    Write-Host "Building SQL Server query..."
    $query = @"
SELECT s.name + '.' + t.name AS source_table_name,
       SUM(p.rows) AS row_count
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
JOIN sys.partitions p ON t.object_id = p.object_id
WHERE p.index_id IN (0, 1)
$schemaCondition
GROUP BY s.name, t.name
ORDER BY source_table_name;
"@

    try {
        Write-Host "Executing SQL Server query..."
        $results = Invoke-Sqlcmd -ConnectionString $connStr -Query $query
        $results | Format-Table -AutoSize
        Write-Host "SQL Server query completed."
    } catch {
        Write-Error "SQL Server query failed: $_"
    }
}


function Get-PostgresTableCounts($pgConn, $pgMeta) {
    $dbName = $pgMeta.database
    $schema = $pgMeta.schema
    Write-Host "`nPostgreSQL - Database: $dbName, Schema: $schema" -ForegroundColor Cyan

    $tableQuery = @"
SELECT table_name
FROM information_schema.tables
WHERE table_type = 'BASE TABLE'
  AND table_schema = '$schema';
"@

    try {
        $env:PGPASSWORD = $pgConn.password

        $tableNames = & psql -h $pgConn.host -p $pgConn.port -U $pgConn.username -d $dbName -t -A -c "$tableQuery"

        if (-not $tableNames) {
            Write-Host "No tables found in schema '$schema'."
            return
        }

        $tableList = $tableNames -split "`n" | Where-Object { $_ -ne "" }

        $tableCounts = @()

        foreach ($table in $tableList) {
            $qualifiedTable = "$schema.`"$table`""
            $countQuery = "SELECT COUNT(*) FROM $qualifiedTable;"
            $rowCount = & psql -h $pgConn.host -p $pgConn.port -U $pgConn.username -d $dbName -t -A -c "$countQuery"

            $tableCounts += [PSCustomObject]@{
                Table     = "$dbName.$table"
                RowCount  = [int]$rowCount
            }
        }

        # Sort by table name (alphabetically)
        $sortedCounts = $tableCounts | Sort-Object Table

        Write-Host "`ntarget_table_name`t`t`trow_count"
        Write-Host "-----------------`t`t`t---------"
        foreach ($entry in $sortedCounts) {
            Write-Host "$($entry.Table)`t`t$($entry.RowCount)"
        }

        Remove-Item Env:\PGPASSWORD
        Write-Host "`nPostgreSQL query completed."
    } catch {
        Write-Error "PostgreSQL query failed: $_"
    }
}


# Main
$configPath = ".\db_cred.conf"
$metaPath = ".\db_metadata.conf"

Write-Host "Starting database row count script..." -ForegroundColor Green

if (-Not (Test-Path $configPath)) {
    Write-Error "Connection config file '$configPath' not found."
    exit
}
if (-Not (Test-Path $metaPath)) {
    Write-Error "Metadata config file '$metaPath' not found."
    exit
}

Write-Host "Loading configuration files..."
$connConfig = Read-Config $configPath
$metaConfig = Read-Config $metaPath

# SQL Server
if ($connConfig.ContainsKey("SQLServer") -and $metaConfig.ContainsKey("SQLServer")) {
    Write-Host "Processing SQL Server connection..." -ForegroundColor Yellow
    Get-SqlServerTableCounts $connConfig["SQLServer"] $metaConfig["SQLServer"]
} else {
    Write-Warning "SQL Server config or metadata missing."
}

# PostgreSQL
if ($connConfig.ContainsKey("PostgreSQL") -and $metaConfig.ContainsKey("PostgreSQL")) {
    Write-Host "Processing PostgreSQL connection..." -ForegroundColor Yellow
    Get-PostgresTableCounts $connConfig["PostgreSQL"] $metaConfig["PostgreSQL"]
} else {
    Write-Warning "PostgreSQL config or metadata missing."
}

Write-Host "`nScript execution completed." -ForegroundColor Green
