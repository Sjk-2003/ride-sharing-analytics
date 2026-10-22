# Health check (Windows PowerShell):
#   powershell -ExecutionPolicy Bypass -File scripts\check-laptop.ps1
kubectl get pods
Write-Host "`n== Producer logs (last 5) =="
kubectl logs deployment/ride-producer --tail=5
Write-Host "`n== 3 messages from Kafka topic =="
kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic ride-requests --from-beginning --max-messages 3
Write-Host "`n== SwiftRide website health (http://localhost:30080/api/health) =="
try {
    Invoke-RestMethod -Uri "http://localhost:30080/api/health" -TimeoutSec 5 | Format-List
} catch {
    Write-Host "Website not answering on port 30080: $($_.Exception.Message)" -ForegroundColor Red
}
