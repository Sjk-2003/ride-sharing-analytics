"""
Live ride statistics (the "speed layer").

Keeps running totals of every ride seen on Kafka, never the rides themselves,
so memory stays tiny no matter how many rides arrive. Only the last 50 rides
are kept, for the live feed.

Plain Python only (no Kafka, no web framework) so it can be tested on its own.
"""
import threading
import time
from collections import deque

RECENT_SIZE = 50          # rides shown in the live feed
THROUGHPUT_WINDOW = 60    # seconds, for "rides in the last minute"
CANCEL_ALERT_PCT = 18     # cancellation rate that should worry Customer Support


def _hour_label(h):
    return f"{h:02d}:00"


def inr(amount):
    """Rupees in Indian style: 342269 → Rs.3,42,269 (shown as the rupee sign)."""
    n = str(int(round(amount)))
    neg, n = n.startswith("-"), n.lstrip("-")
    head, tail = n[:-3], n[-3:]
    while len(head) > 2:
        tail = head[-2:] + "," + tail
        head = head[:-2]
    return ("-" if neg else "") + "₹" + (head + "," + tail if head else tail)


def _share(part, whole):
    return round(100.0 * part / whole, 1) if whole else 0.0


class RideStats:
    def __init__(self):
        self.lock = threading.Lock()
        self.reset_count = 0           # how many times the Kafka topic was reset
        self._clear()

    # ------------------------------------------------------------------ state
    def _clear(self):
        self.total = 0
        self.completed = 0
        self.cancelled = 0
        self.revenue = 0.0
        self.bad_messages = 0
        # hour of day (from request_time) → requests, revenue, surge rides
        self.hours = [{"requests": 0, "revenue": 0.0, "surge_rides": 0} for _ in range(24)]
        self.drivers = {}    # driver_id → {"rides", "earnings", "km"} (completed rides only)
        self.zones = {}      # pickup_zone → {"requests", "completed", "revenue"}
        self.vehicles = {}   # vehicle_type → {"requests", "completed", "revenue"}
        self.surge = {"surge_rides": 0, "surge_revenue": 0.0,
                      "normal_rides": 0, "normal_revenue": 0.0}
        self.recent = deque(maxlen=RECENT_SIZE)
        self.arrivals = deque()        # Kafka arrival times (seconds) within the last minute
        self.last_message_at = None    # Kafka arrival time of the newest ride
        self.last_offset = -1

    def reset(self, count=True):
        """Throw away all totals (Kafka topic starts again from the beginning)."""
        with self.lock:
            self._clear()
            if count:
                self.reset_count += 1

    # ------------------------------------------------------------------ input
    def add(self, ride, arrival_ts=None, offset=None):
        """Add one ride (the JSON dict from Kafka). arrival_ts = Kafka timestamp in seconds."""
        if arrival_ts is None:
            arrival_ts = time.time()
        with self.lock:
            # Offsets only go up. If one goes backwards, Kafka was restarted and the
            # topic is new, so throw away the old totals and count again from zero.
            if offset is not None:
                if offset < self.last_offset:
                    self._clear()
                    self.reset_count += 1
                self.last_offset = offset
            try:
                self._add(ride, arrival_ts)
            except (KeyError, TypeError, ValueError):
                self.bad_messages += 1

    def _add(self, r, arrival_ts):
        status = r["status"]
        fare = float(r["fare"] or 0)
        km = float(r["distance_km"] or 0)
        surge = float(r["surge_multiplier"] or 1) > 1.0
        hour = int(str(r["request_time"])[11:13])
        if not 0 <= hour <= 23:
            raise ValueError("bad hour")
        done = status == "completed"

        self.total += 1
        if done:
            self.completed += 1
            self.revenue += fare
        elif status == "cancelled":
            self.cancelled += 1

        h = self.hours[hour]
        h["requests"] += 1
        h["revenue"] += fare if done else 0.0
        if surge:
            h["surge_rides"] += 1

        if done:
            d = self.drivers.setdefault(r["driver_id"], {"rides": 0, "earnings": 0.0, "km": 0.0})
            d["rides"] += 1
            d["earnings"] += fare
            d["km"] += km
            key = "surge" if surge else "normal"
            self.surge[key + "_rides"] += 1
            self.surge[key + "_revenue"] += fare

        for table, name in ((self.zones, r["pickup_zone"]), (self.vehicles, r["vehicle_type"])):
            t = table.setdefault(name, {"requests": 0, "completed": 0, "revenue": 0.0})
            t["requests"] += 1
            if done:
                t["completed"] += 1
                t["revenue"] += fare

        self.recent.appendleft({
            "ride_id": r.get("ride_id"), "driver_id": r.get("driver_id"),
            "pickup_zone": r["pickup_zone"], "drop_zone": r.get("drop_zone"),
            "vehicle_type": r["vehicle_type"], "distance_km": km, "fare": fare,
            "surge": surge, "status": status, "request_time": r["request_time"],
        })
        self.arrivals.append(arrival_ts)
        self._trim_arrivals(time.time())
        if self.last_message_at is None or arrival_ts > self.last_message_at:
            self.last_message_at = arrival_ts

    def _trim_arrivals(self, now):
        # While replaying old messages at start-up they are all older than a
        # minute, so they fall out straight away and don't inflate "live" speed.
        while self.arrivals and self.arrivals[0] < now - THROUGHPUT_WINDOW:
            self.arrivals.popleft()

    # ----------------------------------------------------------------- output
    def summary(self):
        now = time.time()
        self._trim_arrivals(now)
        return {
            "total_requests": self.total,
            "completed": self.completed,
            "cancelled": self.cancelled,
            "cancellation_rate_pct": _share(self.cancelled, self.total),
            "total_revenue": round(self.revenue, 2),
            "avg_fare": round(self.revenue / self.completed, 2) if self.completed else 0.0,
            "rides_last_minute": len(self.arrivals),
            "last_message_at": self.last_message_at,
            "last_message_age_s": round(now - self.last_message_at, 1) if self.last_message_at else None,
        }

    def peak_hours(self):
        return [{"hour": i, "label": _hour_label(i), "requests": h["requests"],
                 "revenue": round(h["revenue"], 2), "surge": h["surge_rides"] > 0}
                for i, h in enumerate(self.hours)]

    def top_drivers(self, n=10):
        ranked = sorted(self.drivers.items(), key=lambda kv: (-kv[1]["rides"], -kv[1]["earnings"]))
        return [{"rank": i + 1, "driver_id": d, "rides": v["rides"],
                 "earnings": round(v["earnings"], 2), "km": round(v["km"], 1)}
                for i, (d, v) in enumerate(ranked[:n])]

    def zones_list(self):
        return sorted(({"zone": z, "requests": v["requests"], "rides": v["completed"],
                        "revenue": round(v["revenue"], 2)} for z, v in self.zones.items()),
                      key=lambda x: -x["revenue"])

    def vehicles_list(self):
        return sorted(({"vehicle_type": k, "requests": v["requests"], "rides": v["completed"],
                        "revenue": round(v["revenue"], 2),
                        "avg_fare": round(v["revenue"] / v["completed"], 2) if v["completed"] else 0.0}
                       for k, v in self.vehicles.items()),
                      key=lambda x: -x["revenue"])

    def pricing(self):
        s = self.surge
        rides = s["surge_rides"] + s["normal_rides"]
        rev = s["surge_revenue"] + s["normal_revenue"]
        return {
            "surge_rides": s["surge_rides"], "normal_rides": s["normal_rides"],
            "surge_revenue": round(s["surge_revenue"], 2), "normal_revenue": round(s["normal_revenue"], 2),
            "surge_revenue_share_pct": _share(s["surge_revenue"], rev),
            "surge_ride_share_pct": _share(s["surge_rides"], rides),
            "surge_avg_fare": round(s["surge_revenue"] / s["surge_rides"], 2) if s["surge_rides"] else 0.0,
            "normal_avg_fare": round(s["normal_revenue"] / s["normal_rides"], 2) if s["normal_rides"] else 0.0,
        }

    def insights(self, summary, hours, drivers, zones, vehicles, pricing):
        """One plain-English 'what to do' sentence per team, built from the live numbers."""
        if summary["total_requests"] == 0:
            wait = "Waiting for the first rides from Kafka..."
            return {k: [wait] for k in ("overview", "ops", "pricing", "drivers", "city")}

        out = {}
        # Overview / management
        rate = summary["cancellation_rate_pct"]
        out["overview"] = [
            f"Cancellation rate is {rate}% "
            + (f"- above the {CANCEL_ALERT_PCT}% alert level: Customer Support should review cancelled trips."
               if rate > CANCEL_ALERT_PCT else f"- below the {CANCEL_ALERT_PCT}% alert level, no action needed.")
        ]

        # Operations: when to put more drivers on the road
        busiest = max(hours, key=lambda h: h["requests"])
        quietest = min(hours, key=lambda h: h["requests"])
        am = max(hours[:12], key=lambda h: h["requests"])
        pm = max(hours[12:], key=lambda h: h["requests"])
        avg = summary["total_requests"] / 24
        busy = [h for h in hours if h["requests"] >= 1.5 * avg]
        out["ops"] = [
            f"Busiest hour is {busiest['label']}-{(busiest['hour'] + 1) % 24:02d}:00 with {busiest['requests']:,} requests "
            f"({_share(busiest['requests'], summary['total_requests'])}% of the day).",
        ]
        if am["requests"] and pm["requests"]:
            out["ops"].append(f"Two rush peaks: morning around {am['label']} and evening around {pm['label']} "
                              f"→ schedule more drivers in these windows.")
        out["ops"].append(f"{len(busy)} hours run at 1.5x the average demand or more; "
                          f"quietest is {quietest['label']} → best time for driver breaks and vehicle servicing.")

        # Pricing
        top_rev = max(hours, key=lambda h: h["revenue"])
        out["pricing"] = [
            f"Surge hours bring {pricing['surge_revenue_share_pct']}% of revenue from only "
            f"{pricing['surge_ride_share_pct']}% of completed rides.",
            f"Highest-earning hour is {top_rev['label']}.",
        ]
        if pricing["surge_rides"] and pricing["normal_rides"]:
            out["pricing"].insert(1,
                f"Average surge fare {inr(pricing['surge_avg_fare'])} vs normal {inr(pricing['normal_avg_fare'])} "
                f"→ keep 1.5x surge in rush hours; test a small surge on shoulder hours next to them.")

        # Driver relations
        if drivers:
            top3 = drivers[:3]
            top3_rides = sum(d["rides"] for d in top3)
            all_rides = sum(v["rides"] for v in self.drivers.values())
            mean = all_rides / len(self.drivers)
            low = sum(1 for v in self.drivers.values() if v["rides"] < 0.5 * mean)
            out["drivers"] = [
                f"Reward the top 3 this week: {', '.join(d['driver_id'] for d in top3)} "
                f"({_share(top3_rides, all_rides)}% of all completed rides).",
                f"{len(self.drivers)} drivers active; {low} drove less than half the average "
                f"({mean:.0f} rides) → offer them peak-hour incentives.",
            ]
        else:
            out["drivers"] = ["No completed rides yet."]

        # City & fleet
        city = []
        if len(zones) > 1:
            best, worst = zones[0], zones[-1]
            city.append(f"{best['zone']} earns the most ({inr(best['revenue'])}); "
                        f"{worst['zone']} the least ({inr(worst['revenue'])}) "
                        f"→ run rider promotions in {worst['zone']}.")
        if vehicles:
            v = vehicles[0]
            done = sum(x["rides"] for x in vehicles)
            rev = sum(x["revenue"] for x in vehicles)
            most_used = max(vehicles, key=lambda x: x["rides"])
            city.append(f"{v['vehicle_type']} brings {_share(v['revenue'], rev)}% of revenue from "
                        f"{_share(v['rides'], done)}% of rides; {most_used['vehicle_type']} is used most "
                        f"→ grow the {v['vehicle_type']} fleet first.")
        out["city"] = city or ["No completed rides yet."]
        return out

    def snapshot(self):
        """Everything the website needs, in one consistent copy."""
        with self.lock:
            s = self.summary()
            hours = self.peak_hours()
            drivers = self.top_drivers()
            zones = self.zones_list()
            vehicles = self.vehicles_list()
            pricing = self.pricing()
            return {
                "summary": s,
                "peak_hours": hours,
                "drivers": drivers,
                "zones": zones,
                "vehicles": vehicles,
                "pricing": pricing,
                "recent": list(self.recent),
                "insights": self.insights(s, hours, drivers, zones, vehicles, pricing),
                "reset_count": self.reset_count,
                "bad_messages": self.bad_messages,
                "last_offset": self.last_offset,
            }
