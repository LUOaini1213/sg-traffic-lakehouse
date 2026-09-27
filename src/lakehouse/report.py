"""Static HTML report from the gold layer (results/report.html). Charts are PNGs embedded in the page."""
from __future__ import annotations

import base64
import html
import io
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from .storage import duckdb_connect  # noqa: E402

INK, MUTED, GRID = "#1f2933", "#616e7c", "#e4e7eb"
SERIES = ["#2563eb", "#d97706", "#059669", "#9333ea"]


def _png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=110, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode()


def _style(ax, title, ylabel):
    ax.set_title(title, loc="left", fontsize=11, color=INK)
    ax.set_ylabel(ylabel, color=MUTED, fontsize=9)
    ax.grid(axis="y", color=GRID, linewidth=0.8)
    ax.spines[["top", "right"]].set_visible(False)
    ax.tick_params(colors=MUTED, labelsize=8)


def chart_congestion(con) -> str | None:
    rows = con.execute("""
        select s.snapshot_ts, s.sampling_run, l.road_category,
               avg(case when o.speed_band <= 2 then 1.0 else 0.0 end) as share
        from silver.stg_speedband_obs o join silver.dim_snapshot s using (snapshot_ts)
        join silver.dim_link l using (link_id)
        where l.road_category in (1, 2, 3) and o.speed_band is not null
        group by all order by 3, 2, 1""").fetchall()
    if not rows:
        return None
    fig, ax = plt.subplots(figsize=(8, 3.2))
    labels = {1: "Expressways", 2: "Major arterials", 3: "Arterials"}
    for i, cat in enumerate((1, 2, 3)):
        runs = sorted({r[1] for r in rows if r[2] == cat})
        for j, run in enumerate(runs):
            pts = [(r[0], r[3] * 100) for r in rows if r[2] == cat and r[1] == run]
            ax.plot([p[0] for p in pts], [p[1] for p in pts], marker="o", ms=3, lw=1.5, color=SERIES[i],
                    label=labels[cat] if j == 0 else None)
    _style(ax, "Share of links at band 1-2 (under 20 km/h), per snapshot", "% of links")
    ax.legend(frameon=False, fontsize=8)
    fig.autofmt_xdate()
    return _png(fig)


def chart_demand(con) -> str | None:
    rows = con.execute("""
        select year_month, day_type, hour, sum(avg_daily_tap_in) as taps
        from gold.gold_stop_demand_hourly group by all order by 1, 2, 3""").fetchall()
    if not rows:
        return None
    fig, axes = plt.subplots(1, 2, figsize=(8, 3), sharey=True)
    months = sorted({r[0] for r in rows})
    for ax, dtype in zip(axes, ("WEEKDAY", "WEEKENDS/HOLIDAY")):
        for i, ym in enumerate(months):
            pts = [(r[2], r[3] / 1000) for r in rows if r[0] == ym and r[1] == dtype]
            ax.plot([p[0] for p in pts], [p[1] for p in pts], lw=1.5, color=SERIES[i % 4], label=ym)
        _style(ax, f"Bus tap-ins per {'weekday' if dtype == 'WEEKDAY' else 'weekend/holiday day'}",
               "thousand tap-ins per hour")
        ax.set_xlabel("hour of day", color=MUTED, fontsize=9)
    axes[0].legend(frameon=False, fontsize=8)
    return _png(fig)


def _table(headers, rows) -> str:
    th = "".join(f"<th>{html.escape(str(h))}</th>" for h in headers)
    tr = "".join("<tr>" + "".join(f"<td>{html.escape(str(c))}</td>" for c in r) + "</tr>" for r in rows)
    return f"<table><thead><tr>{th}</tr></thead><tbody>{tr}</tbody></table>"


def run(settings) -> str:
    con = duckdb_connect(settings, settings.warehouse, read_only=True)
    res = Path(settings.results_dir)
    parts = []
    c1, c2 = chart_congestion(con), chart_demand(con)
    if c1:
        parts.append(f'<h2>Congestion on major roads</h2><img alt="congestion share" src="data:image/png;base64,{c1}">'
                     "<p>Each line segment is one sampling run; breaks are sampling gaps.</p>")
    ep = con.execute("""
        select l.road_category_label, count(*), round(avg(e.n_snapshots), 1), max(e.n_snapshots)
        from gold.gold_congestion_episodes e join silver.dim_link l using (link_id)
        group by all order by min(l.road_category)""").fetchall()
    parts.append("<h2>Congestion episodes by road category</h2>" +
                 _table(["Road category", "Episodes", "Mean snapshots", "Longest (snapshots)"], ep))
    if c2:
        parts.append(f'<h2>Stop demand</h2><img alt="tap-ins by hour" src="data:image/png;base64,{c2}">')
    od = con.execute("""
        select flow_rank, origin_stop_code || ' ' || coalesce(origin_stop_name, '?'),
               destination_stop_code || ' ' || coalesce(destination_stop_name, '?'), avg_daily_trips
        from gold.gold_top_od_flows
        where year_month = (select max(year_month) from gold.gold_top_od_flows) and day_type = 'WEEKDAY'
        order by flow_rank limit 10""").fetchall()
    ym = con.execute("select max(year_month) from gold.gold_top_od_flows").fetchone()[0]
    parts.append(f"<h2>Top origin-destination flows, weekdays, {html.escape(str(ym))}</h2>" +
                 _table(["Rank", "Origin", "Destination", "Trips per weekday"], od))
    con.close()
    q = res / "quality_report.json"
    if q.exists():
        checks = json.loads(q.read_text(encoding="utf-8"))["checks"]
        parts.append("<h2>Data quality</h2>" + _table(["Check", "Severity", "Status", "Value", "Threshold"],
                     [[c["check"], c["severity"], c["status"], c["value"], c["threshold"]] for c in checks]))
    page = f"""<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1"><title>SG traffic lakehouse report</title>
<style>
:root {{ --ink:{INK}; --muted:{MUTED}; --line:{GRID}; --bg:#ffffff; }}
body {{ font: 14px/1.5 system-ui, sans-serif; color: var(--ink); background: var(--bg); max-width: 900px;
       margin: 0 auto; padding: 16px; }}
h1 {{ font-size: 20px; }} h2 {{ font-size: 16px; margin-top: 28px; }}
img {{ max-width: 100%; height: auto; }} p {{ color: var(--muted); }}
table {{ border-collapse: collapse; width: 100%; font-size: 13px; display: block; overflow-x: auto; }}
th, td {{ text-align: left; padding: 4px 8px; border-bottom: 1px solid var(--line); }}
</style></head><body>
<h1>Singapore traffic lakehouse: gold-layer report</h1>
<p>Built by <code>python -m lakehouse report</code> from the gold tables. Sources: LTA DataMall.</p>
{''.join(parts)}
</body></html>"""
    out = res / "report.html"
    res.mkdir(parents=True, exist_ok=True)
    out.write_text(page, encoding="utf-8")
    return str(out)
