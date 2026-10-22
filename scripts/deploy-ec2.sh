#!/bin/bash
# Builds the images and deploys Kafka + producer + SwiftRide website to k3s.
# Safe to run again any time (e.g. after the EC2 restarts and gets a new IP).
set -e
cd "$(dirname "$0")/.."

# Find this EC2's current public IP (changes after every stop/start)
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
PUBLIC_IP=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/public-ipv4)
echo "EC2 public IP: $PUBLIC_IP"

echo "== Building producer Docker image =="
sudo docker build -t ride-producer:v1 ./producer
echo "== Building website Docker image =="
sudo docker build -t swiftride-dashboard:v1 ./dashboard
echo "== Loading images into Kubernetes (k3s) =="
sudo docker save ride-producer:v1 | sudo k3s ctr images import -
sudo docker save swiftride-dashboard:v1 | sudo k3s ctr images import -

echo "== Deploying Kafka =="
sed "s/__EXTERNAL_ADDRESS__/$PUBLIC_IP:30094/g" k8s/kafka.yaml | kubectl apply -f -
kubectl rollout status deployment/kafka --timeout=300s

echo "== Deploying producer =="
kubectl apply -f k8s/producer.yaml
kubectl rollout restart deployment/ride-producer
kubectl rollout status deployment/ride-producer --timeout=120s

echo "== Deploying SwiftRide website =="
kubectl apply -f k8s/dashboard.yaml
kubectl rollout restart deployment/swiftride-dashboard
kubectl rollout status deployment/swiftride-dashboard --timeout=180s

kubectl get pods,svc
echo ""
free -h
echo ""
echo "============================================================"
echo " Paste this into the Databricks notebook (KAFKA_BOOTSTRAP):"
echo "   $PUBLIC_IP:30094"
echo ""
echo " Live website (open on your laptop or phone):"
echo "   http://$PUBLIC_IP:30080"
echo "   (needs the security-group rule: TCP 30080 from 0.0.0.0/0)"
echo "============================================================"
