# System walkthrough

A guided tour of the running pipeline, from the business problem to the stored data.

## The business problem
SwiftRide Bengaluru runs bikes, autos and cabs in 10 zones. Ride requests arrive every second, and four
business functions need answers:
- **Operations**: when do we need more drivers on the road?
- **Pricing**: when should surge pricing apply?
- **Driver Relations**: which drivers should be rewarded, and which need support?
- **Expansion & Fleet**: where should the service grow, and with which vehicles?

A daily spreadsheet arrives too late, so the system provides **live numbers** and a **reliable history**.

## 1. Rides entering Kafka
```bash
kubectl logs deployment/ride-producer -f
```
The producer stands in for the mobile app. It sends 2 ride requests per second and prints
`Sent N rides | latest: …` every 50 rides.

## 2. Speed layer: the live dashboard
Open `http://<host>:30080`. The KPIs keep rising and new rides slide into the feed. The green **LIVE**
indicator shows that the dashboard is reading Kafka directly, with no manual refresh.

| Tab | Who uses it | What they read | Decision it supports |
|---|---|---|---|
| Overview | Management | KPIs, cancellation rate vs the 18% alert level | Escalate to customer support if cancellations rise |
| Operations | Ops manager | Busiest hours, rush peaks (orange = surge) | More drivers at 8–10 AM and 5–8 PM, breaks in the quietest hours |
| Pricing | Pricing & revenue | Surge share of revenue vs share of rides, fare by hour | Keep 1.5× surge at rush hours, test a small surge in the hours next to them |
| Driver Relations | Driver relations | Top 10 leaderboard, low-activity drivers | Reward the top performers, offer peak-hour incentives to low-activity drivers |
| City & Fleet | Expansion & fleet | Revenue by zone, fleet mix | Promotions in the weakest zone, grow the highest-earning vehicle type |

## 3. Batch layer: Databricks
1. Run **Step 2** and note `Total rides stored so far: N`. Wait 30 seconds and run it again. **N goes up**,
   because the checkpoint picks up only the new rides.
2. Run **Step 3** to build the Gold tables and view the peak-hours chart.

## 4. Data lake: S3
Run **Step 5**. A new timestamped folder appears in the bucket with six CSV files: five Gold tables plus the raw rides.
This data can be used for finance reports and future ML models.

## Troubleshooting
| Symptom | Fix |
|---|---|
| Dashboard indicator is red: "No new rides" | `kubectl get pods`, then `kubectl rollout restart deployment/ride-producer` |
| Dashboard does not open | Check the security-group rule for TCP 30080, then run `kubectl get pods` and the check script |
| A pod shows `OOMKilled` or the machine is very slow | Out of memory. Use at least a t3.small and confirm that swap is on. |
| Databricks Step 1 prints FAILED | The address changed. Paste the new `KAFKA_BOOTSTRAP` and run the RESET cell once. |
