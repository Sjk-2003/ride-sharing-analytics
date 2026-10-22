#!/bin/bash
# Run ONCE on a fresh Ubuntu 24.04 t3.small:  bash scripts/setup_ec2.sh
set -e

echo "== 1/3 Adding 2 GB swap (backup memory so Kafka + k3s don't crash) =="
if [ ! -f /swapfile ]; then
  sudo fallocate -l 2G /swapfile
  sudo chmod 600 /swapfile
  sudo mkswap /swapfile
  sudo swapon /swapfile
  echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
fi
free -h

echo "== 2/3 Installing Docker =="
sudo apt-get update -y
sudo apt-get install -y docker.io
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"

echo "== 3/3 Installing k3s (lightweight Kubernetes) =="
# traefik + metrics-server switched off to save memory
curl -sfL https://get.k3s.io | INSTALL_K3S_EXEC="--disable traefik --disable metrics-server --write-kubeconfig-mode 644" sh -

echo "Waiting for Kubernetes node to be Ready..."
sleep 20
kubectl get nodes
echo "Setup finished. Next: bash scripts/deploy-ec2.sh"
