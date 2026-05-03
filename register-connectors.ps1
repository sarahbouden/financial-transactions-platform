$ErrorActionPreference = "Stop"
$baseUrl = "http://localhost:8083"

# 1. Wait for Kafka Connect REST API
Write-Host "Waiting for Kafka Connect to be ready..."
for ($i = 1; $i -le 30; $i++) {
    try {
        $null = Invoke-RestMethod -Uri "$baseUrl/connectors" -Method Get
        Write-Host "Kafka Connect is ready."
        break
    } catch {
        if ($i -eq 30) {
            Write-Error "Kafka Connect did not respond after 30 attempts. Check: docker logs kafka-connect"
            exit 1
        }
        Write-Host "  Attempt $i/30 - not ready yet, retrying in 10s..."
        Start-Sleep -Seconds 10
    }
}

# 2. Verify both plugins are loaded
Write-Host ""
Write-Host "Verifying installed plugins..."
$plugins = Invoke-RestMethod -Uri "$baseUrl/connector-plugins" -Method Get
$s3Plugin     = $plugins | Where-Object { $_.class -like "*S3SinkConnector" }
$pubsubPlugin = $plugins | Where-Object { $_.class -like "*CloudPubSubSinkConnector" }

if (-not $s3Plugin) {
    Write-Warning "S3SinkConnector plugin NOT found. Rebuild: docker-compose build --no-cache kafka-connect"
} else {
    Write-Host "  [OK] S3SinkConnector found."
}

if (-not $pubsubPlugin) {
    Write-Warning "CloudPubSubSinkConnector plugin NOT found. Rebuild: docker-compose build --no-cache kafka-connect"
} else {
    Write-Host "  [OK] CloudPubSubSinkConnector found."
}

if (-not $s3Plugin -or -not $pubsubPlugin) {
    Write-Error "One or more plugins missing. Fix the image and re-run this script."
    exit 1
}

# 3. Register connectors via PUT (create-or-update / idempotent)
foreach ($file in @("s3-sink-connector.json", "pubsub-sink-connector.json")) {
    $json       = Get-Content -Path $file -Raw | ConvertFrom-Json
    $name       = $json.name
    $configBody = $json.config | ConvertTo-Json -Depth 10

    Write-Host ""
    Write-Host "Registering '$name'..."
    Invoke-RestMethod -Method Put `
        -Uri "$baseUrl/connectors/$name/config" `
        -ContentType "application/json" `
        -Body $configBody | Out-Null
    Write-Host "  Registered '$name'."
}

# 4. Verify connector status
Write-Host ""
Write-Host "Waiting 8s for tasks to initialise..."
Start-Sleep -Seconds 8

Write-Host ""
Write-Host "============== Connector Status =============="
$allRunning = $true
foreach ($name in @("s3-sink-connector", "pubsub-sink-connector")) {
    $s = Invoke-RestMethod -Uri "$baseUrl/connectors/$name/status"
    $connState = $s.connector.state
    Write-Host "$name"
    Write-Host "  connector : $connState"
    if ($connState -ne "RUNNING") { $allRunning = $false }
    foreach ($task in $s.tasks) {
        Write-Host "  task[$($task.id)]  : $($task.state)"
        if ($task.trace) {
            Write-Host "  ERROR     : $($task.trace)" -ForegroundColor Red
            $allRunning = $false
        }
    }
}
Write-Host "=============================================="

if ($allRunning) {
    Write-Host ""
    Write-Host "Both connectors are RUNNING." -ForegroundColor Green
} else {
    Write-Warning "One or more connectors are not RUNNING. Check logs: docker logs kafka-connect"
}
