// SwiftRide Bengaluru - Live Operations Center (frontend)
// Polls /api/all every 2 seconds and updates the page in place.
(function () {
  "use strict";

  var POLL_MS = 2000;
  var STALE_S = 10;          // no new ride for this long -> amber
  var inr0 = new Intl.NumberFormat("en-IN", { style: "currency", currency: "INR", maximumFractionDigits: 0 });
  var num = new Intl.NumberFormat("en-IN");
  var $ = function (id) { return document.getElementById(id); };
  var hasChart = typeof window.Chart !== "undefined";

  var last = null;            // last good /api/all response
  var lastFetchOk = 0;        // browser time of last good response
  var fetchFailed = false;
  var seenRides = new Set();
  var charts = {};

  // Schematic Bengaluru layout (roughly by geography: north at top, east to the right)
  var ZONE_POS = {
    "Yelahanka": [190, 40], "Hebbal": [195, 115], "MG Road": [165, 195], "Indiranagar": [260, 175],
    "Whitefield": [365, 180], "Marathahalli": [320, 245], "Koramangala": [215, 260], "HSR Layout": [285, 320],
    "Jayanagar": [110, 280], "Electronic City": [235, 385]
  };

  // ------------------------------------------------------------ helpers
  function css(name) { return getComputedStyle(document.documentElement).getPropertyValue(name).trim(); }
  function esc(s) { return String(s == null ? "" : s).replace(/[&<>"]/g, function (c) { return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]; }); }
  function setText(id, text) {
    var el = $(id);
    if (el && el.textContent !== text) {
      el.textContent = text;
      if (el.classList.contains("kpi-value")) { el.classList.remove("flash"); void el.offsetWidth; el.classList.add("flash"); }
    }
  }
  function list(id, items) {
    var html = (items || []).map(function (t) { return "<li>" + esc(t) + "</li>"; }).join("");
    var el = $(id); if (el && el.innerHTML !== html) el.innerHTML = html;
  }
  function since(s) {
    if (s == null) return "no rides yet";
    if (s < 1) return "just now";
    if (s < 90) return Math.round(s) + "s ago";
    return Math.round(s / 60) + " min ago";
  }

  // ------------------------------------------------------------ tabs
  function showTab(name) {
    var buttons = document.querySelectorAll("#tabs button");
    var found = false;
    buttons.forEach(function (b) { if (b.dataset.tab === name) found = true; });
    if (!found) name = "overview";
    buttons.forEach(function (b) { b.setAttribute("aria-selected", b.dataset.tab === name ? "true" : "false"); });
    document.querySelectorAll(".panel").forEach(function (p) { p.hidden = p.id !== "tab-" + name; });
    if (location.hash !== "#" + name) history.replaceState(null, "", "#" + name);
    if (last) render(last);  // charts need a visible canvas to size correctly
  }
  $("tabs").addEventListener("click", function (e) {
    var b = e.target.closest("button"); if (b) showTab(b.dataset.tab);
  });
  window.addEventListener("hashchange", function () { showTab(location.hash.slice(1)); });

  // ------------------------------------------------------------ charts
  function baseOptions(yMoney, horizontal) {
    var grid = css("--grid"), text = css("--text-2");
    var valueAxis = {
      beginAtZero: true, grid: { color: grid }, border: { display: false },
      ticks: { color: text, callback: function (v) { return yMoney ? inr0.format(v) : num.format(v); } }
    };
    var catAxis = { grid: { display: false }, border: { color: grid }, ticks: { color: text, autoSkip: true, maxRotation: 0 } };
    return {
      responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
      indexAxis: horizontal ? "y" : "x",
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: function (ctx) {
              var v = horizontal ? ctx.parsed.x : ctx.parsed.y;
              return " " + (yMoney ? inr0.format(v) : num.format(v) + " rides");
            }
          }
        }
      },
      scales: horizontal ? { x: valueAxis, y: catAxis } : { x: catAxis, y: valueAxis },
      datasets: { bar: { borderRadius: 4, borderSkipped: "start", maxBarThickness: 36, categoryPercentage: 0.85, barPercentage: 0.9 } }
    };
  }
  function upsertChart(key, canvasId, labels, values, colors, money, horizontal) {
    var canvas = $(canvasId);
    if (!canvas || canvas.offsetParent === null) return;  // tab hidden: draw later
    if (!hasChart) {
      if (!canvas.dataset.noChart) {
        canvas.dataset.noChart = "1";
        canvas.parentNode.insertAdjacentHTML("beforeend", '<div class="no-chart">Chart library could not load (offline?). Numbers are in the tables and insights.</div>');
      }
      return;
    }
    var c = charts[key];
    if (!c) {
      c = charts[key] = new Chart(canvas, {
        type: "bar",
        data: { labels: labels, datasets: [{ data: values, backgroundColor: colors }] },
        options: baseOptions(money, horizontal)
      });
      return;
    }
    c.data.labels = labels;
    c.data.datasets[0].data = values;
    c.data.datasets[0].backgroundColor = colors;
    c.update("none");
  }
  // Theme changed (OS dark mode): rebuild charts with the new colours
  if (window.matchMedia) {
    window.matchMedia("(prefers-color-scheme: dark)").addEventListener("change", function () {
      Object.keys(charts).forEach(function (k) { charts[k].destroy(); delete charts[k]; });
      if (last) render(last);
    });
  }

  // ------------------------------------------------------------ render
  function renderLive() {
    var el = $("live");
    var h = last && last.health;
    var age = last && last.summary.last_message_age_s;
    var extra = lastFetchOk ? (Date.now() - lastFetchOk) / 1000 : 0;
    var state, label;
    if (fetchFailed && (!lastFetchOk || Date.now() - lastFetchOk > 6000)) { state = "down"; label = "Website server unreachable"; }
    else if (!h) { state = "wait"; label = "Connecting…"; }
    else if (!h.kafka_connected) { state = "down"; label = "Kafka disconnected"; }
    else if (age == null) { state = "stale"; label = "Waiting for rides"; }
    else if (age + extra > STALE_S) { state = "down"; label = "No new rides"; }
    else { state = "ok"; label = "LIVE"; }
    el.dataset.state = state;
    setText("liveLabel", label);
    setText("liveAge", age == null ? "" : "Last update " + since(age + extra));
  }

  function renderFeed(recent) {
    var feed = $("feed");
    var first = seenRides.size === 0;
    var html = recent.slice(0, 12).map(function (r) {
      var isNew = !first && !seenRides.has(r.ride_id);
      var tags = '<span class="tag">' + esc(r.vehicle_type) + "</span>" +
        (r.surge ? '<span class="tag surge">surge 1.5×</span>' : "") +
        (r.status === "cancelled" ? '<span class="tag cancel">cancelled</span>' : "");
      return '<li class="' + (isNew ? "new " : "") + esc(r.status) + '">' +
        '<span class="route">' + esc(r.pickup_zone) + " → " + esc(r.drop_zone) + tags + "</span>" +
        '<span class="fare">' + (r.status === "completed" ? inr0.format(r.fare) : "—") + "</span>" +
        '<span class="meta">' + esc(r.driver_id) + " · " + esc(r.distance_km) + " km · requested " + esc(String(r.request_time).slice(11, 16)) + "</span>" +
        "</li>";
    }).join("");
    if (feed.innerHTML !== html) feed.innerHTML = html || '<li class="meta">Waiting for the first rides from Kafka…</li>';
    seenRides = new Set(recent.map(function (r) { return r.ride_id; }));
  }

  function hourColors(hours) {
    var s1 = css("--series-1"), s2 = css("--series-2");
    return hours.map(function (h) { return h.surge ? s2 : s1; });
  }

  function renderSplit(id, surgePct) {
    var normalPct = Math.round((100 - surgePct) * 10) / 10;
    $(id).innerHTML =
      '<div class="surge" style="flex-grow:' + Math.max(surgePct, 0.5) + '" title="Surge ' + surgePct + '%">' + surgePct + "%</div>" +
      '<div class="normal" style="flex-grow:' + Math.max(normalPct, 0.5) + '" title="Normal ' + normalPct + '%">' + normalPct + "%</div>";
  }

  function renderDrivers(drivers) {
    var max = drivers.length ? drivers[0].rides : 1;
    $("drivers").innerHTML = drivers.map(function (d) {
      return '<tr class="' + (d.rank <= 3 ? "top" : "") + '"><td><span class="rank">' + d.rank + "</span></td>" +
        "<td><b>" + esc(d.driver_id) + "</b></td>" +
        '<td class="num">' + num.format(d.rides) + "</td>" +
        '<td class="num">' + inr0.format(d.earnings) + "</td>" +
        '<td class="num">' + num.format(Math.round(d.km)) + "</td>" +
        '<td class="bar-col"><div class="bar" style="width:' + (100 * d.rides / max).toFixed(1) + '%"></div></td></tr>';
    }).join("") || '<tr><td colspan="6" class="muted">No completed rides yet.</td></tr>';
  }

  function renderVehicles(vehicles) {
    var total = vehicles.reduce(function (a, v) { return a + v.revenue; }, 0) || 1;
    $("vehicles").innerHTML = vehicles.map(function (v) {
      var pct = 100 * v.revenue / total;
      return "<tr><td><b>" + esc(v.vehicle_type) + "</b></td>" +
        '<td class="num">' + num.format(v.rides) + "</td>" +
        '<td class="num">' + inr0.format(v.revenue) + "</td>" +
        '<td class="num">' + inr0.format(v.avg_fare) + "</td>" +
        '<td class="bar-col"><div class="barline"><div class="track"><div class="bar" style="width:' + pct.toFixed(1) + '%"></div></div><span class="muted">' + pct.toFixed(1) + "%</span></div></td></tr>";
    }).join("") || '<tr><td colspan="5" class="muted">No completed rides yet.</td></tr>';
  }

  function renderMap(zones) {
    var el = $("map");
    if (!el || el.offsetParent === null) return;
    var W = 420, H = 440;
    var max = zones.reduce(function (a, z) { return Math.max(a, z.revenue); }, 0) || 1;
    var byZone = {}; zones.forEach(function (z) { byZone[z.zone] = z; });
    var parts = ['<svg viewBox="0 0 ' + W + " " + H + '" role="img" aria-label="Revenue by zone on a schematic map of Bengaluru">',
      '<ellipse class="city" cx="215" cy="215" rx="200" ry="205"/>'];
    Object.keys(ZONE_POS).forEach(function (name) {
      var p = ZONE_POS[name], z = byZone[name] || { revenue: 0, rides: 0 };
      var x = p[0], y = p[1];
      var r = 5 + 17 * Math.sqrt(z.revenue / max);   // area ~ revenue
      parts.push('<g><title>' + esc(name) + ": " + inr0.format(z.revenue) + " from " + num.format(z.rides) + ' rides</title>' +
        '<circle cx="' + x.toFixed(1) + '" cy="' + y.toFixed(1) + '" r="' + r.toFixed(1) + '"/>' +
        '<text x="' + x.toFixed(1) + '" y="' + (y + r + 12).toFixed(1) + '" text-anchor="middle">' + esc(name) + "</text>" +
        '<text class="val" x="' + x.toFixed(1) + '" y="' + (y + r + 24).toFixed(1) + '" text-anchor="middle">' + inr0.format(z.revenue) + "</text></g>");
    });
    parts.push("</svg>");
    el.innerHTML = parts.join("");
  }

  function badge(id, cls, text) { var b = $(id); b.className = "badge " + cls; b.textContent = text; }

  function render(d) {
    var s = d.summary, h = d.health;
    // Overview
    setText("kTotal", num.format(s.total_requests));
    setText("kCompleted", num.format(s.completed));
    setText("kCancel", s.cancellation_rate_pct + "%");
    setText("kRevenue", inr0.format(s.total_revenue));
    setText("kAvg", inr0.format(s.avg_fare));
    setText("kRpm", num.format(s.rides_last_minute));
    renderFeed(d.recent);
    list("insOverview", d.insights.overview);
    var all = [["Operations", d.insights.ops[0]], ["Pricing", d.insights.pricing[0]],
               ["Driver Relations", d.insights.drivers[0]], ["City & Fleet", d.insights.city[0]]];
    var allHtml = all.map(function (p) { return "<li><b>" + esc(p[0]) + "</b>" + esc(p[1] || "") + "</li>"; }).join("");
    if ($("insAll").innerHTML !== allHtml) $("insAll").innerHTML = allHtml;

    // Operations
    var hours = d.peak_hours, labels = hours.map(function (x) { return x.label; });
    upsertChart("hours", "cHours", labels, hours.map(function (x) { return x.requests; }), hourColors(hours), false);
    list("insOps", d.insights.ops);

    // Pricing
    var p = d.pricing;
    renderSplit("splitRev", p.surge_revenue_share_pct);
    renderSplit("splitRides", p.surge_ride_share_pct);
    setText("pSurgeAvg", inr0.format(p.surge_avg_fare));
    setText("pNormalAvg", inr0.format(p.normal_avg_fare));
    upsertChart("revHours", "cRevHours", labels, hours.map(function (x) { return x.revenue; }), hourColors(hours), true);
    list("insPricing", d.insights.pricing);

    // Drivers
    renderDrivers(d.drivers);
    list("insDrivers", d.insights.drivers);

    // City & fleet (zones sorted by revenue; one colour: the bar length is the message)
    upsertChart("zones", "cZones", d.zones.map(function (z) { return z.zone; }),
      d.zones.map(function (z) { return z.revenue; }), css("--series-1"), true, true);
    renderMap(d.zones);
    renderVehicles(d.vehicles);
    list("insCity", d.insights.city);

    // How it works
    badge("bWeb", "ok", "Running");
    badge("bKafka", h.kafka_connected ? (h.topic_found ? "ok" : "warn") : "bad",
      h.kafka_connected ? (h.topic_found ? "Connected" : "Connected, topic not created yet") : "Disconnected");
    badge("bProducer", s.rides_last_minute > 0 ? "ok" : "bad",
      s.rides_last_minute > 0 ? "Sending " + s.rides_last_minute + " rides/min" : "No rides in the last minute");
    setText("healthLine", "Kafka " + h.kafka_bootstrap + " · " + num.format(h.messages_consumed) +
      " messages read since this pod started · topic resets seen: " + h.topic_resets_seen +
      (h.last_error && !h.kafka_connected ? " · last error: " + h.last_error : ""));
    setText("footInfo", num.format(s.total_requests) + " rides in Kafka right now");
  }

  // ------------------------------------------------------------ polling
  function poll() {
    fetch("api/all", { cache: "no-store" })
      .then(function (r) { if (!r.ok) throw new Error(r.status); return r.json(); })
      .then(function (d) { last = d; lastFetchOk = Date.now(); fetchFailed = false; render(d); renderLive(); })
      .catch(function () { fetchFailed = true; renderLive(); })
      .then(function () { setTimeout(poll, POLL_MS); });
  }
  setInterval(renderLive, 1000);   // keep "Xs ago" ticking between polls
  showTab(location.hash.slice(1) || "overview");
  poll();
})();
