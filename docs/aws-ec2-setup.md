# AWS EC2 setup (k3s)

This runs the same code on a single EC2 instance with k3s, a lightweight Kubernetes distribution.
Kafka is reachable at the instance's public IP, so no tunnel is needed.

## 1. Launch the instance
- **t3.small**, **Ubuntu 24.04**, 20 GB disk (the examples use region `ap-south-1`)
- Security group inbound rules:
  | Port | Source | Purpose |
  |---|---|---|
  | 22 | My IP | SSH |
  | 30094 | 0.0.0.0/0 | Kafka external listener (Databricks) |
  | 30080 | 0.0.0.0/0 | Live dashboard |

  > Opening ports to `0.0.0.0/0` is only suitable for a demo. For anything longer-lived, restrict the
  > sources and add TLS and authentication to Kafka.

## 2. Copy the project and connect
```bash
scp -i your-key.pem -r ride-sharing-analytics ubuntu@<EC2_PUBLIC_IP>:~
ssh -i your-key.pem ubuntu@<EC2_PUBLIC_IP>
```

## 3. Install and deploy
```bash
cd ride-sharing-analytics
bash scripts/setup_ec2.sh    # once: 2 GB swap + Docker + k3s
bash scripts/deploy-ec2.sh   # builds images, deploys, prints <public-ip>:30094
bash scripts/check-ec2.sh    # pods, memory, sample messages, dashboard health
```

`deploy-ec2.sh` finds the current public IP from the EC2 instance metadata service (IMDSv2). Because of this,
it can be re-run safely after every stop and start, even though the IP changes each time.

## 4. Connect Databricks
Paste `<public-ip>:30094` into `KAFKA_BOOTSTRAP` in the notebook. Run the RESET cell once, then run everything.

Dashboard: `http://<EC2_PUBLIC_IP>:30080`

## Cost tip
Stop the instance when you are not using it. After a restart, run `bash scripts/deploy-ec2.sh` again.

## Alternative without Kubernetes
If k3s gives trouble, Docker Compose runs Kafka and the producer with the same external port:
```bash
PUBLIC_IP=$(curl -s ifconfig.me) sudo -E docker compose up -d --build
```
