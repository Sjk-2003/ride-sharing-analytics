# Local setup (Docker Desktop Kubernetes)

This setup runs Kafka, the producer and the dashboard on a laptop. A free TCP tunnel lets Databricks reach Kafka.

## 1. Enable Kubernetes in Docker Desktop
1. Install **Docker Desktop**. On Windows, use the **WSL 2** backend.
2. Go to **Settings → Kubernetes → Enable Kubernetes**. Choose **kubeadm** (single node) if asked, then click *Apply & restart*.
3. Check that it works:
   ```powershell
   kubectl get nodes      # should show: docker-desktop   Ready
   ```

## 2. Start a public TCP tunnel
Databricks runs in the cloud, so it needs a public address for Kafka's external port (`30094`).
[Pinggy](https://pinggy.io) works through plain `ssh`, with no signup. A free tunnel lasts 60 minutes.

**Terminal 1** (leave this running):
```powershell
ssh -p 443 -R0:127.0.0.1:30094 tcp@free.pinggy.io
```
It prints an address like `tcp://abcde-12-34.a.free.pinggy.link:41234`. Copy the part after `tcp://`.

> ngrok also works: run `ngrok tcp 30094`, then run the deploy script without `-Address`. The script reads the address from ngrok's local API.

## 3. Deploy
**Terminal 2**, from the repository root:
```powershell
powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1 -Address abcde-12-34.a.free.pinggy.link:41234
powershell -ExecutionPolicy Bypass -File scripts\check-laptop.ps1
```
On macOS or Linux, use `bash scripts/deploy-laptop.sh <host:port>` and `bash scripts/check-laptop.sh`.

The deploy script builds both images and deploys Kafka with the tunnel address as its external listener.
At the end it prints the value to use for `KAFKA_BOOTSTRAP`.

Dashboard: **http://localhost:30080**

To rebuild only the dashboard (Kafka and its data are left untouched):
```powershell
powershell -ExecutionPolicy Bypass -File scripts\deploy-laptop.ps1 -DashboardOnly
```

## 4. Databricks
1. Go to **Workspace → Import** and import `databricks/ride_analytics.py`.
2. Set `KAFKA_BOOTSTRAP` to the tunnel address.
3. Run the cells in order. **Step 1 must print `SUCCESS`.** Then run Step 2 and the analytics cells.

> On Databricks Free Edition, outbound internet access must be enabled (identity verification) before the notebook can reach an external Kafka.

## When the tunnel expires or the laptop restarts
The tunnel gets a new address each time:
1. Start the tunnel again (Terminal 1).
2. Re-run `deploy-laptop.ps1 -Address <new address>`. Kafka restarts, and its old messages are cleared.
3. Paste the new address into the notebook. Run the **OPTIONAL RESET** cell once with `RESET_CHECKPOINT = True`.
