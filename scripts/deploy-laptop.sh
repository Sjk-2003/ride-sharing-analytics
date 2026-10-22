#!/bin/bash
# Laptop version (Mac / Linux): Docker Desktop Kubernetes + a public tunnel
#   bash scripts/deploy-laptop.sh <tunnel-host:port>     # e.g. a Pinggy address
#   bash scripts/deploy-laptop.sh                        # uses a running "ngrok tcp 30094"
#   bash scripts/deploy-laptop.sh localhost:30094        # website test only, no Databricks
set -e
cd "$(dirname "$0")/.."

kubectl config use-context docker-desktop
kubectl get nodes

ADDR="${1#tcp://}"
if [ -z "$ADDR" ]; then
  ADDR=$(curl -s http://127.0.0.1:4040/api/tunnels | grep -o 'tcp://[^"]*' | head -1 | sed 's#tcp://##')
fi
if [ -z "$ADDR" ]; then
  echo "No tunnel address. Give one: bash scripts/deploy-laptop.sh <host:port>  (or start: ngrok tcp 30094)"; exit 1
fi
echo "Tunnel address: $ADDR"

docker build -t ride-producer:v1 ./producer
docker build -t swiftride-dashboard:v1 ./dashboard
sed "s/__EXTERNAL_ADDRESS__/$ADDR/g" k8s/kafka.yaml | kubectl apply -f -
kubectl rollout status deployment/kafka --timeout=300s
kubectl apply -f k8s/producer.yaml
kubectl rollout restart deployment/ride-producer
kubectl rollout status deployment/ride-producer --timeout=120s
kubectl apply -f k8s/dashboard.yaml
kubectl rollout restart deployment/swiftride-dashboard
kubectl rollout status deployment/swiftride-dashboard --timeout=180s
kubectl get pods,svc
echo ""
echo "Paste this into the Databricks notebook (KAFKA_BOOTSTRAP):  $ADDR"
echo "Live website:  http://localhost:30080"
