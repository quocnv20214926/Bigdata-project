"""Generate grafana/dashboards/realtime.json (the provisioned Grafana dashboard).

    python infra/monitoring/make_dashboard.py

Edit the panels here rather than in the JSON, then regenerate.
"""

from __future__ import annotations

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "prometheus"}
OUT = Path(__file__).parent / "grafana" / "dashboards" / "realtime.json"

panels: list[dict] = []
_next_id = 1
_y = 0


def _id() -> int:
    global _next_id
    _next_id += 1
    return _next_id - 1


def row(title: str) -> None:
    global _y
    panels.append({"type": "row", "title": title, "id": _id(), "collapsed": False,
                   "gridPos": {"h": 1, "w": 24, "x": 0, "y": _y}, "panels": []})
    _y += 1


def targets(exprs: list[tuple[str, str]]) -> list[dict]:
    return [{"datasource": DS, "expr": e, "legendFormat": legend, "refId": chr(65 + i), "range": True}
            for i, (e, legend) in enumerate(exprs)]


def timeseries(title, exprs, x, w, h=8, unit="short", stack=False, description=""):
    panels.append({
        "type": "timeseries", "title": title, "id": _id(), "datasource": DS, "description": description,
        "gridPos": {"h": h, "w": w, "x": x, "y": _y},
        "targets": targets(exprs),
        "fieldConfig": {"defaults": {
            "unit": unit,
            "custom": {"drawStyle": "line", "lineWidth": 1, "fillOpacity": 20 if stack else 0,
                       "showPoints": "never", "stacking": {"mode": "normal" if stack else "none"}},
        }, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
                    "tooltip": {"mode": "multi", "sort": "desc"}},
    })


def stat(title, expr, x, w, h=4, unit="short", description="", thresholds=None, decimals=None):
    defaults = {"unit": unit, "color": {"mode": "thresholds"},
                "thresholds": {"mode": "absolute", "steps": thresholds or [{"color": "green", "value": None}]}}
    if decimals is not None:
        defaults["decimals"] = decimals
    panels.append({
        "type": "stat", "title": title, "id": _id(), "datasource": DS, "description": description,
        "gridPos": {"h": h, "w": w, "x": x, "y": _y},
        "targets": targets([(expr, "")]),
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "value", "graphMode": "none", "textMode": "value", "justifyMode": "auto"},
    })


def advance(h: int) -> None:
    global _y
    _y += h


RECOMMEND = 'route="/recommend/{user_id}"'

# ---- overview ----------------------------------------------------------------------
row("Overview")
stat("API requests/s", "sum(rate(api_requests_total[1m]))", 0, 4, unit="reqps", decimals=0)
stat("/recommend p95", f"histogram_quantile(0.95, sum by (le) (rate(api_request_duration_seconds_bucket{{{RECOMMEND}}}[1m])))",
     4, 4, unit="s", description="Target of the plan: p95 < 100 ms",
     thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 0.05}, {"color": "red", "value": 0.1}])
stat("API errors (5xx)", 'sum(rate(api_requests_total{status=~"5.."}[1m])) / clamp_min(sum(rate(api_requests_total[1m])), 1e-9)',
     8, 4, unit="percentunit", thresholds=[{"color": "green", "value": None}, {"color": "red", "value": 0.01}])
stat("Stream clock (event time)", "stream_event_time_seconds * 1000", 12, 4, unit="dateTimeAsIso",
     description="Newest event time processed by the streaming job (the replay is 2019 data)")
stat("Since last micro-batch", 'time() - stream_last_batch_timestamp_seconds{query="events"}', 16, 4, unit="s",
     thresholds=[{"color": "green", "value": None}, {"color": "orange", "value": 30}, {"color": "red", "value": 120}],
     description="Wall-clock seconds since the events query finished a micro-batch (it runs every 2 s while data arrives)")
stat("Events processed", "stream_events_total", 20, 4, decimals=0)
advance(4)

# ---- API ---------------------------------------------------------------------------
row("Recommendation API")
timeseries("Requests/s by endpoint", [("sum by (route) (rate(api_requests_total[1m]))", "{{route}}")], 0, 8, unit="reqps")
timeseries("Latency of /recommend", [
    (f"histogram_quantile({q}, sum by (le) (rate(api_request_duration_seconds_bucket{{{RECOMMEND}}}[1m])))", f"p{int(q * 100)}")
    for q in (0.5, 0.95, 0.99)], 8, 8, unit="s")
timeseries("Recommended items by source", [("sum by (source) (rate(api_recommended_items_total[1m]))", "{{source}}")],
           16, 8, unit="short", stack=True,
           description="batch / realtime / both come from rec:batch and rec:rt; trending / popular are the fallback")
advance(8)
timeseries("/recommend requests by available layer", [
    ('sum(rate(api_recommend_requests_total{batch="true",realtime="true"}[1m]))', "batch + real-time"),
    ('sum(rate(api_recommend_requests_total{batch="true",realtime="false"}[1m]))', "batch only"),
    ('sum(rate(api_recommend_requests_total{batch="false",realtime="true"}[1m]))', "real-time only"),
    ('sum(rate(api_recommend_requests_total{batch="false",realtime="false"}[1m]))', "neither (fallback)"),
], 0, 12, unit="reqps", stack=True)
timeseries("Responses by status", [("sum by (status) (rate(api_requests_total[1m]))", "{{status}}")], 12, 12, unit="reqps")
advance(8)

# ---- speed layer ---------------------------------------------------------------------
row("Speed layer (Kafka → Spark Structured Streaming → Redis)")
timeseries("Input vs processed events/s", [
    ("sum(rate(kafka_topic_end_offset[1m]))", "written to Kafka"),
    ("rate(stream_events_total[1m])", "processed by the events query"),
], 0, 8, unit="short", description="Processed stays a little below input: exact duplicates are dropped")
timeseries("Backlog (approximate)", [("sum(kafka_topic_end_offset) - stream_events_total", "messages not processed yet")],
           8, 8, unit="short",
           description="Messages in the topic minus processed events; includes dropped duplicates and late events, "
                       "so it does not reach exactly zero")
timeseries("Micro-batch duration", [("stream_batch_duration_seconds", "{{query}}")], 16, 8, unit="s")
advance(8)
timeseries("Rows per micro-batch", [("stream_batch_rows", "{{query}}")], 0, 12, unit="short")
timeseries("Event-time progress", [("deriv(stream_event_time_seconds[2m])", "event-time seconds per second")],
           12, 12, unit="short", description="Replay speed actually achieved (60 = one hour of data per minute)")
advance(8)

# ---- Redis ----------------------------------------------------------------------------
row("Redis")
timeseries("Memory", [("redis_used_memory_bytes", "used")], 0, 8, unit="bytes")
timeseries("Keys by prefix", [("redis_keys_by_prefix", "{{prefix}}")], 8, 8, unit="short")
timeseries("Commands/s", [("redis_ops_per_second", "ops/s")], 16, 8, unit="short")
advance(8)

dashboard = {
    "uid": "pair2-realtime",
    "title": "Recommender: real-time pipeline and API",
    "tags": ["recommender", "pair2"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "editable": True,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "templating": {"list": []},
    "annotations": {"list": []},
    "panels": panels,
}

if __name__ == "__main__":
    OUT.write_text(json.dumps(dashboard, indent=2) + "\n")
    print(f"wrote {OUT} ({len(panels)} panels)")
