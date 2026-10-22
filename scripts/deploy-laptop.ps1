# Laptop version (Windows PowerShell): Docker Desktop Kubernetes + a public TCP tunnel
#
# OPTION 1 - Pinggy (free, no card, no signup, tunnel lasts 60 minutes):
#   Window 1:  ssh -p 443 -R0:127.0.0.1:30094 tcp@free.pinggy.io
#   Copy the address it prints (e.g. abcde-1-2-3-4.a.free.pinggy.link:41234), then
#   Window 2:  powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1 -Address abcde-1-2-3-4.a.free.pinggy.link:41234
#
# OPTION 2 - ngrok (needs card verification):
#   Window 1:  ngrok tcp 30094
#   Window 2:  powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1
#
# ONLY TESTING THE WEBSITE (no tunnel, no Databricks): use a dummy address
#   powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1 -Address localhost:30094
#
# ONLY REBUILD THE WEBSITE (Kafka and producer untouched, keeps their data):
#   powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1 -DashboardOnly
param(
    [string]$Address = "",
    [switch]$DashboardOnly
)

Set-Location (Join-Path $PSScriptRoot "..")

Write-Host "== 1/6 Using Docker Desktop's Kubernetes ==" -ForegroundColor Cyan
kubectl config use-context docker-desktop
kubectl get nodes

if (-not $DashboardOnly) {
    Write-Host "== 2/6 Getting the public tunnel address ==" -ForegroundColor Cyan
    if ($Address -ne "") {
        $addr = $Address -replace "^tcp://", ""
    } else {
        try {
            $tunnels = Invoke-RestMethod -Uri "http://127.0.0.1:4040/api/tunnels"
            $tcp = $tunnels.tunnels | Where-Object { $_.proto -eq "tcp" } | Select-Object -First 1
            $addr = $tcp.public_url -replace "^tcp://", ""
        } catch { $addr = "" }
        if (-not $addr) {
            Write-Host "No tunnel address given." -ForegroundColor Red
            Write-Host "Start Pinggy in another window:  ssh -p 443 -R0:127.0.0.1:30094 tcp@free.pinggy.io" -ForegroundColor Yellow
            Write-Host "then run:  powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1 -Address <host:port>" -ForegroundColor Yellow
            Write-Host "(website test only, no Databricks:  ... -Address localhost:30094)" -ForegroundColor Yellow
            exit 1
        }
    }
    Write-Host "Tunnel address: $addr"

    Write-Host "== 3/6 Building producer Docker image ==" -ForegroundColor Cyan
    docker build -t ride-producer:v1 ./producer
    if ($LASTEXITCODE -ne 0) { Write-Host "Docker build failed" -ForegroundColor Red; exit 1 }
}

Write-Host "== 4/6 Building website Docker image ==" -ForegroundColor Cyan
docker build -t swiftride-dashboard:v1 ./dashboard
if ($LASTEXITCODE -ne 0) { Write-Host "Docker build failed" -ForegroundColor Red; exit 1 }

if (-not $DashboardOnly) {
    Write-Host "== 5/6 Deploying Kafka + producer ==" -ForegroundColor Cyan
    (Get-Content k8s/kafka.yaml -Raw).Replace("__EXTERNAL_ADDRESS__", $addr) | kubectl apply -f -
    kubectl rollout status deployment/kafka --timeout=300s
    kubectl apply -f k8s/producer.yaml
    kubectl rollout restart deployment/ride-producer
    kubectl rollout status deployment/ride-producer --timeout=120s
}

Write-Host "== 6/6 Deploying SwiftRide website ==" -ForegroundColor Cyan
kubectl apply -f k8s/dashboard.yaml
kubectl rollout restart deployment/swiftride-dashboard
kubectl rollout status deployment/swiftride-dashboard --timeout=180s

kubectl get pods,svc
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
if (-not $DashboardOnly) {
    Write-Host " Paste this into the Databricks notebook (KAFKA_BOOTSTRAP):" -ForegroundColor Green
    Write-Host "   $addr" -ForegroundColor Green
    Write-Host ""
}
Write-Host " Live website:  http://localhost:30080" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
