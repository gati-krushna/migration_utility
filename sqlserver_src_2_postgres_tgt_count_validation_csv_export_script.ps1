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
        $results = Invoke-Sqlcmd -ConnectionString $connStr -Query $query
        return $results
    } catch {
        Write-Error "SQL Server query failed: $_"
        return @()
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
        if (-not $tableNames) { return @() }

        #$tableList = $tableNames -split "`n" | Where-Object { $_ -ne "" }
        $tableList = $tableNames -split "`n" | Where-Object { $_ -ne "" } | ForEach-Object { $_.ToLower() }
        $tableCounts = @()

        foreach ($table in $tableList) {
            $qualifiedTable = "$schema.`"$table`""
            $countQuery = "SELECT COUNT(*) FROM $qualifiedTable;"
            $rowCount = & psql -h $pgConn.host -p $pgConn.port -U $pgConn.username -d $dbName -t -A -c "$countQuery"

            $tableCounts += [PSCustomObject]@{
                target_table_name = "$schema.$table"
                row_count         = [int]$rowCount
            }
        }

        Remove-Item Env:\PGPASSWORD
        return ($tableCounts | Sort-Object target_table_name)
    } catch {
        Write-Error "PostgreSQL query failed: $_"
        return @()
    }
}


# Main
$configPath = ".\db_cred.conf"
$metaPath = ".\db_metadata.conf"

$csvOutputPath = ".\source2targetcount.csv"

# Get connection and metadata
$connConfig = Read-Config $configPath
$metaConfig = Read-Config $metaPath

$sqlResults = @()
$pgResults  = @()

if ($connConfig.ContainsKey("SQLServer") -and $metaConfig.ContainsKey("SQLServer")) {
    $sqlResults = Get-SqlServerTableCounts $connConfig["SQLServer"] $metaConfig["SQLServer"]
}
if ($connConfig.ContainsKey("PostgreSQL") -and $metaConfig.ContainsKey("PostgreSQL")) {
    $pgResults = Get-PostgresTableCounts $connConfig["PostgreSQL"] $metaConfig["PostgreSQL"]
}

# Normalize both lists
$maxLength = [Math]::Max($sqlResults.Count, $pgResults.Count)
$combined = for ($i = 0; $i -lt $maxLength; $i++) {
    [PSCustomObject]@{
        source_table_name = if ($i -lt $sqlResults.Count) { $sqlResults[$i].source_table_name } else { "" }
        source_row_count  = if ($i -lt $sqlResults.Count) { $sqlResults[$i].row_count } else { "" }
        target_table_name = if ($i -lt $pgResults.Count)  { $pgResults[$i].target_table_name } else { "" }
        target_row_count  = if ($i -lt $pgResults.Count)  { $pgResults[$i].row_count } else { "" }
    }
}

# Export to CSV
$combined | Export-Csv -Path $csvOutputPath -NoTypeInformation -Encoding UTF8

Write-Host "`ntable counts exported to: $csvOutputPath" -ForegroundColor Green
