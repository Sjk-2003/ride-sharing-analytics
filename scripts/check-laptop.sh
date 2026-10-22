#!/bin/bash
kubectl get pods
echo; echo "== Producer logs (last 5) =="; kubectl logs deployment/ride-producer --tail=5
echo; echo "== 3 messages from Kafka topic =="
kubectl exec deployment/kafka -- /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ride-requests --from-beginning --max-messages 3
echo; echo "== SwiftRide website health (http://localhost:30080/api/health) =="
curl -s --max-time 5 http://localhost:30080/api/health || echo "Website not answering on port 30080"
echo
