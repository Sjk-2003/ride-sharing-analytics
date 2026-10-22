# Ride Sharing Analytics: Real-Time Pipeline with Kafka, Kubernetes, Databricks and AWS

An end-to-end streaming data pipeline for **SwiftRide Bengaluru**, a fictional ride-hailing service with
bikes, autos and cabs across 10 zones of Bengaluru. Ride requests are streamed through **Apache Kafka on
Kubernetes**, shown live on a **FastAPI operations dashboard**, and processed in **Databricks** into
Delta tables that are exported to **Amazon S3**.

The pipeline answers four business questions:
- **When** is demand highest? (peak hours, so more drivers can be scheduled)
- **When** should surge pricing apply, and how much revenue does it bring?
- **Which** drivers are the most active, and which need support?
- **Where** is revenue coming from? (by zone and vehicle type)

---

## Architecture

This is a **Lambda architecture**: one Kafka topic feeds two independent consumers.

```
                    Kubernetes (Docker Desktop locally / k3s on AWS EC2)
 ┌────────────────────────────────────────────────────────────────────┐
 │                                                                    │
 │  Producer pod ──► Kafka pod (KRaft) ──┬──► Dashboard pod (FastAPI) │──► Browser :30080
 │  (simulated rides,   topic:           │    speed layer, 2 s refresh│
 │   2 rides/sec)       ride-requests    │                            │
 └───────────────────────────────────────┼────────────────────────────┘
                                         │ external listener :30094
                                         ▼
                         Databricks Structured Streaming
                         Bronze (raw rides) → Gold (aggregates)
                                         │
                                         ▼
                              Amazon S3 (timestamped CSVs)
                                   batch layer
```

| Layer | Path | Purpose |
|---|---|---|
| **Speed** | Kafka → Dashboard | Live KPIs within seconds, for decisions *now* |
| **Batch** | Kafka → Databricks → Delta → S3 | Cleaned, stored history for reporting and future ML |

## Tech stack

| Area | Tools |
|---|---|
| Streaming | Apache Kafka 3.8 (KRaft mode, no ZooKeeper) |
| Orchestration | Kubernetes (Docker Desktop, k3s), Docker, Docker Compose |
| Processing | Databricks, PySpark Structured Streaming, Delta Lake |
| Storage | Delta tables (Bronze / Gold), Amazon S3 |
| Backend / UI | Python, FastAPI, Uvicorn, vanilla HTML/CSS/JS |
| Cloud | AWS EC2 (t3.small, Ubuntu 24.04), S3, IAM |

---

## Features

### Live operations dashboard
Served on port `30080`. It reads Kafka *inside* the cluster and refreshes every 2 seconds.

| Tab | Audience | Shows |
|---|---|---|
| Overview | Management | KPIs, cancellation rate vs an 18% alert level, live ride feed |
| Operations | Ops manager | Ride requests by hour, with surge hours highlighted |
| Pricing | Pricing & revenue | Surge share of revenue vs share of rides, fare by hour |
| Driver Relations | Driver relations | Top 10 leaderboard, low-activity drivers |
| City & Fleet | Expansion & fleet | Revenue by zone, fleet mix by vehicle type |
| How it works | Everyone | Architecture explanation |

Each tab includes an insight sentence (for example *"Busiest hour is 18:00–19:00 with 1,204 requests…"*).
These sentences are generated from the live numbers, so they change as new rides arrive.

**JSON API:** `/api/all`, `/api/summary`, `/api/peak-hours`, `/api/drivers`, `/api/zones`,
`/api/vehicles`, `/api/recent`, `/api/health`

### Databricks notebook
- **Step 1**: checks that Databricks can reach Kafka over the network
- **Step 2**: streams the topic into a **Bronze** Delta table using a checkpoint and `trigger(availableNow=True)`.
  This works on serverless compute and can be scheduled as a Job.
- **Step 3**: builds **Gold** tables: summary KPIs, peak hours, top drivers, revenue by zone, revenue by vehicle
- **Step 4**: charts for each result
- **Step 5**: exports all Gold tables and the raw rides to S3 as timestamped CSVs

### Simulated data
The producer generates realistic ride requests:

```json
{
  "ride_id": "3f9a1c2e", "driver_id": "D017", "rider_id": "R0842",
  "pickup_zone": "Koramangala", "drop_zone": "Whitefield",
  "vehicle_type": "Cab", "distance_km": 14.2, "surge_multiplier": 1.5,
  "fare": 315.6, "status": "completed", "request_time": "2026-10-05 18:24:51"
}
```

- 50 drivers with uneven workloads (a fixed random seed, so the leaderboard is reproducible)
- Demand weighted toward 8–10 AM and 5–8 PM, with a **1.5× surge** during peak hours
- Fare = (₹40 base + ₹12/km) × vehicle multiplier (Bike 0.6, Auto 0.8, Cab 1.0) × surge
- About 15% of rides are cancelled

`request_time` is a simulated time of day, so peak-hour patterns show up within minutes. `kafka_received_at`
(added in Databricks) is the real time Kafka received each message.

---

## Project structure

```
ride-sharing-analytics/
├── producer/                 # Ride request generator
│   ├── producer.py
│   ├── requirements.txt
│   └── Dockerfile
├── dashboard/                # Live operations dashboard (speed layer)
│   ├── app.py                #   FastAPI app + background Kafka consumer
│   ├── stats.py              #   Running aggregates and insight sentences
│   ├── static/               #   index.html, app.js, style.css
│   ├── requirements.txt
│   └── Dockerfile
├── databricks/
│   └── ride_analytics.py     # Databricks notebook (batch layer)
├── k8s/                      # Kubernetes manifests
│   ├── kafka.yaml            #   Kafka + internal (9092) and external (30094) services
│   ├── producer.yaml
│   └── dashboard.yaml        #   Dashboard + NodePort 30080
├── scripts/                  # Deploy and health-check scripts
│   ├── deploy-laptop.ps1 / .sh
│   ├── check-laptop.ps1 / .sh
│   ├── setup_ec2.sh
│   ├── deploy-ec2.sh
│   └── check-ec2.sh
├── docs/
│   ├── local-setup.md        # Run on a laptop (Docker Desktop Kubernetes)
│   ├── aws-ec2-setup.md      # Run on AWS EC2 with k3s
│   ├── s3-setup.md           # S3 bucket + IAM for the export step
│   └── walkthrough.md        # Guided tour of the running system
└── docker-compose.yml        # Alternative: Kafka + producer without Kubernetes
```

---

## Quick start

### Prerequisites
- Docker Desktop with Kubernetes enabled (or an Ubuntu EC2 instance for the cloud version)
- `kubectl`
- A Databricks workspace (Free Edition works) for the batch layer
- An AWS account for S3 (optional, used only in the export step)

### Run the dashboard locally (no Databricks needed)
```powershell
# Windows
powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1 -Address localhost:30094
powershell -ExecutionPolicy Bypass -File scripts\check-laptop.ps1
```
```bash
# macOS / Linux
bash scripts/deploy-laptop.sh localhost:30094
bash scripts/check-laptop.sh
```
Then open **http://localhost:30080**.

### Full pipeline
- **Laptop + Databricks**: [docs/local-setup.md](docs/local-setup.md). Uses a TCP tunnel so Databricks can reach Kafka.
- **AWS EC2**: [docs/aws-ec2-setup.md](docs/aws-ec2-setup.md). No tunnel needed.
- **S3 export**: [docs/s3-setup.md](docs/s3-setup.md)

---

## Engineering challenges and solutions

| Challenge | Solution |
|---|---|
| Kafka on a 2 GB machine | KRaft mode (no ZooKeeper), 384 MB JVM heap, 2 GB swap, k3s with traefik and metrics-server turned off |
| Kafka needs two addresses | Two listeners: `kafka:9092` inside the cluster and `<host>:30094` for Databricks, with the external address set at deploy time |
| Kubernetes env vars breaking Kafka | `enableServiceLinks: false` stops Kubernetes from injecting `KAFKA_*` variables |
| Databricks reaching a laptop | Kafka's NodePort is exposed through a TCP tunnel (Pinggy or ngrok) |
| EC2 public IP changes on restart | The deploy script reads the IP from instance metadata (IMDSv2) on every run |
| Kafka has no persistent volume | The dashboard notices when the topic resets and clears its totals. The notebook has a checkpoint reset cell. |
| Serverless-only Databricks compute | `trigger(availableNow=True)` micro-batches instead of a long-running stream |
| Dashboard totals kept in memory | A single replica and a single Uvicorn worker. Totals are rebuilt from the topic on every start. |

## Future improvements
- **Demand forecasting**: train on the S3 history to predict rides per zone and hour
- **Dynamic surge pricing**: set prices from live demand and supply per zone instead of fixed hours
- **Driver matching and ETA prediction**
- **Fraud detection**: flag unusual cancellations or abnormal fares in the stream
- **Production hardening**: a 3-broker Kafka cluster with persistent volumes, TLS, IAM roles instead of access keys, HTTPS, CI/CD

## Useful commands
```bash
kubectl get pods                              # are all pods Running?
kubectl logs deployment/ride-producer -f      # watch rides being produced
kubectl logs deployment/kafka --tail=30       # Kafka errors
kubectl describe pod <pod-name>               # why a pod is stuck
```

## Author

**Sowmya J**

📧 somiyajin@gmail.com
