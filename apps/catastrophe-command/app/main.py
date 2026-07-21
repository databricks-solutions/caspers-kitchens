from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import random
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import (
    Disposition,
    ExecuteStatementRequestOnWaitTimeout,
    Format,
)
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from . import db
from .agent import Agent

log = logging.getLogger(__name__)

app = FastAPI(title="Catastrophe Command Center", version="4.0.0")


@app.on_event("startup")
def _startup() -> None:
    # Open the Lakebase pool and ensure the orders/statuses/refunds/complaints
    # schema. Non-fatal: the sim runs client-side even if Lakebase is unavailable.
    db.init_db()
    cfg = db.get_config()
    if cfg and cfg.get("city"):
        _mirror_active_city_to_uc(str(cfg["city"]))

CATALOG = os.environ.get("DATABRICKS_CATALOG", "devconnect")
SIMULATOR_SCHEMA = os.environ.get("SIMULATOR_SCHEMA", "simulator")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
DEFAULT_SCENARIO = os.environ.get("CATASTROPHE_SCENARIO", "bridge_outage")
CATASTROPHE_SEED = os.environ.get("CATASTROPHE_SEED", "2026")
AI_GATEWAY_ENDPOINT_NAME = os.environ.get("AI_GATEWAY_ENDPOINT_NAME", "")
OPS_WAREHOUSE_NAME = os.environ.get("OPS_WAREHOUSE_NAME", "")
APP_BUILDER_URL = os.environ.get("APP_BUILDER_URL", "")
GENIE_SPACE_URL = os.environ.get("GENIE_SPACE_URL", "")
CUSTOM_DASHBOARD_URL = os.environ.get("CUSTOM_DASHBOARD_URL", "")
SLACK_CHANNEL_HINT = os.environ.get("SLACK_CHANNEL_HINT", "#devconnect-casper-demo")
DEMO_THEATER = os.environ.get("DEMO_THEATER", "amsterdam").strip().lower()

# Active DevConnect tour city. CITY_CONFIG is a JSON blob (bridge geography,
# banks, customer regions) produced by the Catastrophe_Command stage from the
# single-source city registry in data/canonical/catastrophe_scenarios.py.
CITY = os.environ.get("CITY", "amsterdam").strip().lower()
CITY_NAME = os.environ.get("CITY_NAME", "Amsterdam, Netherlands")

# Fallback matches the Amsterdam entry in the registry so the app still works if
# CITY_CONFIG is somehow unset (e.g. app started before a fresh stage run).
_AMSTERDAM_CITY_CONFIG: dict[str, Any] = {
    "id": "amsterdam",
    "name": "Amsterdam, Netherlands",
    "flag": "🇳🇱",
    "river": "Amstel",
    "center": [52.351755, 4.909375],
    "bridge": {"name": "Berlagebrug", "coord": [52.34734, 4.9127]},
    "alt": {"name": "Nieuwe Amstelbrug", "coord": [52.35617, 4.90605]},
    "regions": {"a": [52.342459177553344, 4.895332789164471], "b": [52.35222082244666, 4.930067210835529]},
    "approaches": {"a": [52.34651401466287, 4.909760933550911], "b": [52.34816598533713, 4.915639066449089]},
    "close_radius_m": 120,
    "catastrophe": {
        "icon": "🌉",
        "title": "Berlagebrug structural failure",
        "desc": "The Berlagebrug's bascule mechanism seized and a span support cracked. The Amstel crossing is shut for emergency structural checks.",
        "label": "closed",
    },
}


def _city_config() -> dict[str, Any]:
    raw = os.environ.get("CITY_CONFIG", "").strip()
    if raw:
        try:
            return json.loads(raw)
        except (ValueError, TypeError):
            pass
    return _AMSTERDAM_CITY_CONFIG


# ─────────────────────────────────────────────────────────────────────────────
# City registry — MIRRORS data/canonical/catastrophe_scenarios.py (CITIES,
# city_config, generate_city_kitchens). It's duplicated here because the app is
# deployed with only apps/catastrophe-command/app on its runtime path and cannot
# import the canonical module. Keep the two in sync when editing either. This is
# what lets the in-app config screen switch cities without re-running the stage.
# Each tuple: (name, flag, river, bridge_name, (lat,lon), alt_name, (lat,lon), close_radius_m)
# ─────────────────────────────────────────────────────────────────────────────
_CITIES: dict[str, dict[str, Any]] = {
    "amsterdam":    {"name": "Amsterdam, Netherlands", "flag": "🇳🇱", "river": "Amstel",         "bridge_name": "Berlagebrug",                    "bridge": (52.34734, 4.91270),   "alt_name": "Nieuwe Amstelbrug",          "alt": (52.35617, 4.90605),   "r": 120},
    "montreal":     {"name": "Montreal, QC",           "flag": "🇨🇦", "river": "St. Lawrence",    "bridge_name": "Jacques Cartier Bridge",         "bridge": (45.52144, -73.54056), "alt_name": "Victoria Bridge",            "alt": (45.48699, -73.54471), "r": 220},
    "sao_paulo":    {"name": "São Paulo, Brazil",      "flag": "🇧🇷", "river": "Pinheiros",       "bridge_name": "Ponte Estaiada",                 "bridge": (-23.61211, -46.69962),"alt_name": "Ponte Cidade Jardim",        "alt": (-23.58684, -46.69217),"r": 200},
    "vienna":       {"name": "Vienna, Austria",        "flag": "🇦🇹", "river": "Danube",          "bridge_name": "Reichsbrücke",                   "bridge": (48.22802, 16.40921),  "alt_name": "Floridsdorfer Brücke",       "alt": (48.24944, 16.38958),  "r": 220},
    "warsaw":       {"name": "Warsaw, Poland",         "flag": "🇵🇱", "river": "Vistula",         "bridge_name": "Poniatowski Bridge",             "bridge": (52.23565, 21.03829),  "alt_name": "Świętokrzyski Bridge",       "alt": (52.24147, 21.03331),  "r": 200},
    "paris":        {"name": "Paris, France",          "flag": "🇫🇷", "river": "Seine",           "bridge_name": "Pont de la Concorde",            "bridge": (48.86338, 2.31960),   "alt_name": "Pont Alexandre III",         "alt": (48.86347, 2.31352),   "r": 130},
    "washington_dc":{"name": "Washington, DC",         "flag": "🇺🇸", "river": "Potomac",         "bridge_name": "Francis Scott Key Bridge",       "bridge": (38.90456, -77.06885), "alt_name": "Theodore Roosevelt Bridge",  "alt": (38.89229, -77.05984), "r": 200},
    "boston":       {"name": "Boston, MA",             "flag": "🇺🇸", "river": "Charles",         "bridge_name": "Longfellow Bridge",              "bridge": (42.36144, -71.07361), "alt_name": "Harvard Bridge",             "alt": (42.35455, -71.09130), "r": 170},
    "bangalore":    {"name": "Bangalore, India",       "flag": "🇮🇳", "river": "Outer Ring Road", "bridge_name": "Silk Board Junction",            "bridge": (12.91582, 77.62404),  "alt_name": "Agara Junction",             "alt": (12.92214, 77.64794),  "r": 200},
    "seoul":        {"name": "Seoul, South Korea",     "flag": "🇰🇷", "river": "Han",             "bridge_name": "Banpo Bridge",                   "bridge": (37.51456, 126.99651), "alt_name": "Hannam Bridge",              "alt": (37.52671, 127.01338), "r": 260},
    "tokyo":        {"name": "Tokyo, Japan",           "flag": "🇯🇵", "river": "Sumida",          "bridge_name": "Kachidoki Bridge",               "bridge": (35.66226, 139.77490), "alt_name": "Eitai Bridge",               "alt": (35.67664, 139.78615), "r": 200},
    "chicago":      {"name": "Chicago, IL",            "flag": "🇺🇸", "river": "Chicago River",   "bridge_name": "DuSable Bridge",                 "bridge": (41.88882, -87.62439), "alt_name": "Wells Street Bridge",        "alt": (41.88755, -87.63399), "r": 140},
    "minneapolis":  {"name": "Minneapolis, MN",        "flag": "🇺🇸", "river": "Mississippi",     "bridge_name": "I-35W St. Anthony Falls Bridge", "bridge": (44.97948, -93.24479), "alt_name": "Hennepin Avenue Bridge",     "alt": (44.98533, -93.26386), "r": 200},
}

# Hardcoded, city-specific catastrophe per tour stop. Each ties to the city's
# real choke point (the primary bridge/junction that closes). icon+title+desc
# drive the in-app push banner; label is the short status on the choke marker.
# MIRRORS CITY_CATASTROPHES in data/canonical/catastrophe_scenarios.py.
_CITY_CATASTROPHES: dict[str, dict[str, str]] = {
    "amsterdam":     {"icon": "🌉", "title": "Berlagebrug structural failure",                "desc": "The Berlagebrug's bascule mechanism seized and a span support cracked. The Amstel crossing is shut for emergency structural checks.", "label": "closed"},
    "montreal":      {"icon": "🧊", "title": "Ice storm shuts the Jacques Cartier Bridge",    "desc": "Freezing rain has glazed the deck and ice is falling from the superstructure. The St. Lawrence crossing is fully closed.", "label": "iced over"},
    "sao_paulo":     {"icon": "🌊", "title": "Flash flood on Marginal Pinheiros",             "desc": "A torrential downpour has flooded the Pinheiros riverside; the Ponte Estaiada approaches are underwater.", "label": "flooded"},
    "vienna":        {"icon": "🌉", "title": "Reichsbrücke pier collapse",                    "desc": "A pier has given way and a span has dropped into the Danube. The Reichsbrücke is gone.", "label": "collapsed"},
    "warsaw":        {"icon": "💣", "title": "WWII bomb found by the Poniatowski Bridge",     "desc": "Construction crews uncovered unexploded WWII ordnance. A bomb-disposal cordon has closed the Vistula crossing.", "label": "cordoned off"},
    "paris":         {"icon": "📢", "title": "Protest blocks the Pont de la Concorde",        "desc": "A mass manifestation has flooded Place de la Concorde and blocked the Seine crossing.", "label": "blocked"},
    "washington_dc": {"icon": "🚓", "title": "Security lockdown on the Key Bridge",           "desc": "A presidential motorcade and Secret Service closure have sealed the Francis Scott Key Bridge over the Potomac.", "label": "locked down"},
    "boston":        {"icon": "🚇", "title": "Red Line derailment on the Longfellow",         "desc": "An MBTA train has derailed on the Longfellow Bridge, which carries the Red Line over the Charles. The bridge is closed.", "label": "derailed"},
    "bangalore":     {"icon": "🚗", "title": "Silk Board gridlock meltdown",                  "desc": "Monsoon waterlogging has turned the Silk Board Junction into total gridlock across the Outer Ring Road.", "label": "gridlocked"},
    "seoul":         {"icon": "🚗", "title": "Major accident on the Banpo Bridge",            "desc": "A multi-vehicle pile-up has blocked all lanes across the Han River on the Banpo Bridge.", "label": "blocked"},
    "tokyo":         {"icon": "📡", "title": "Seismic sensor malfunction shuts the Kachidoki Bridge", "desc": "A faulty seismic sensor triggered a false earthquake alert; the Kachidoki Bridge over the Sumida was automatically shut and awaits inspection.", "label": "closed"},
    "chicago":       {"icon": "🌉", "title": "DuSable Bridge stuck open",                     "desc": "A bascule-lift malfunction during a boat run has left the DuSable Bridge jammed upright over the Chicago River.", "label": "stuck open"},
    "minneapolis":   {"icon": "❄️", "title": "Blizzard pile-up on I-35W",                     "desc": "Whiteout conditions have caused a chain-reaction crash on the I-35W St. Anthony Falls Bridge over the Mississippi.", "label": "blocked"},
}

_AREAS = [
    "Downtown", "Riverside", "Old Town", "Harbour", "Uptown", "Midtown",
    "North End", "South Side", "East Bank", "West Bank", "Market", "Station",
    "Garden District", "Parkside", "Hillside", "Bayview", "Central", "Latin Quarter",
    "Heights", "Wharf", "The Commons", "Terrace", "Crossing", "Grand Plaza",
]


def _meters_per_deg(lat: float) -> tuple[float, float]:
    return 111320.0, 111320.0 * math.cos(math.radians(lat))


def _offset(point: tuple[float, float], east_m: float, north_m: float) -> tuple[float, float]:
    mlat, mlon = _meters_per_deg(point[0])
    return (point[0] + north_m / mlat, point[1] + east_m / mlon)


def _across_river_unit(bridge: tuple[float, float], alt: tuple[float, float]) -> tuple[float, float]:
    mlat, mlon = _meters_per_deg(bridge[0])
    dx = (alt[1] - bridge[1]) * mlon
    dy = (alt[0] - bridge[0]) * mlat
    length = math.hypot(dx, dy)
    if length < 1.0:
        return 1.0, 0.0
    return -dy / length, dx / length


def _city_config_for(city_id: str) -> dict[str, Any]:
    """Full geography config for a city id, derived from its two crossings."""
    city = _CITIES.get(city_id)
    if city is None:
        return _city_config()
    bridge, alt = city["bridge"], city["alt"]
    center = ((bridge[0] + alt[0]) / 2.0, (bridge[1] + alt[1]) / 2.0)
    px, py = _across_river_unit(bridge, alt)
    region_a = _offset(bridge, px * 1300, py * 1300)
    region_b = _offset(bridge, -px * 1300, -py * 1300)
    approach_a = _offset(bridge, px * 220, py * 220)
    approach_b = _offset(bridge, -px * 220, -py * 220)
    return {
        "id": city_id,
        "name": city["name"],
        "flag": city["flag"],
        "river": city["river"],
        "center": [center[0], center[1]],
        "bridge": {"name": city["bridge_name"], "coord": [bridge[0], bridge[1]]},
        "alt": {"name": city["alt_name"], "coord": [alt[0], alt[1]]},
        "regions": {"a": [region_a[0], region_a[1]], "b": [region_b[0], region_b[1]]},
        "approaches": {"a": [approach_a[0], approach_a[1]], "b": [approach_b[0], approach_b[1]]},
        "close_radius_m": city["r"],
        "catastrophe": _CITY_CATASTROPHES.get(city_id, _CITY_CATASTROPHES["amsterdam"]),
    }


def _generate_city_kitchens(city_id: str, *, count: int = 24) -> list[dict[str, Any]]:
    """Deterministic kitchen spread around a city centre — fallback when the
    catastrophe_kitchens table has no rows for that city. The client snaps the
    coordinates to real roads on load."""
    city = _CITIES.get(city_id)
    if city is None:
        return []
    cfg = _city_config_for(city_id)
    center = cfg["center"]
    rng = random.Random(f"{city_id}-{CATASTROPHE_SEED}")
    rows: list[dict[str, Any]] = []
    for i in range(count):
        angle = rng.uniform(0, 2 * math.pi)
        dist = 2200.0 * math.sqrt(rng.random())
        lat, lon = _offset((center[0], center[1]), dist * math.cos(angle), dist * math.sin(angle))
        area = _AREAS[i % len(_AREAS)]
        rows.append({
            "kitchen_id": f"{city_id}-{i + 1:02d}", "name": f"Casper's {area}",
            "neighborhood": area, "city": city["name"], "location_id": 0,
            "lat": round(lat, 6), "lon": round(lon, 6), "address": "",
        })
    return rows

# Canonical locations.parquet uses `name`, `lat`, `lon` — not city/latitude/longitude.
THEATER_CONFIG: dict[str, dict[str, Any]] = {
    "amsterdam": {
        "label": "Amsterdam Metro",
        "location_ids": [7, 8],
        "center": [52.2, 4.9],
        "zoom": 11,
        "bbox": (51.92, 4.72, 52.42, 5.18),  # min_lat, min_lon, max_lat, max_lon
    },
    "global": {
        "label": "Global Network",
        "location_ids": [],
        "center": [20, 0],
        "zoom": 2,
        "bbox": None,
    },
}

# Scenario-specific map overlays for the Amsterdam demo theater.
SCENARIO_ZONES: dict[str, dict[str, Any]] = {
    "bridge_outage": {
        "zone_label": "IJ crossing closed",
        "zone_story": "The river crossing is shut. East–west deliveries cannot use the normal route.",
        "center": [52.3798, 4.9025],
        "radius_m": 1400,
    },
    "city_center_accident": {
        "zone_label": "Damrak accident",
        "zone_story": "Multi-vehicle crash blocks downtown arteries near the central kitchen.",
        "center": [52.3745, 4.8979],
        "radius_m": 900,
    },
    "city_center_protest": {
        "zone_label": "Protest perimeter",
        "zone_story": "Police cordons around the city center force long detours for drivers.",
        "center": [52.3702, 4.8951],
        "radius_m": 1100,
    },
    "tomato_supply_shock": {
        "zone_label": "Supply route disrupted",
        "zone_story": "Tomato deliveries halted — menus need substitutions and refunds spike.",
        "center": [51.988, 5.0895],
        "radius_m": 1600,
    },
}

SCENARIO_BRIEF: dict[str, dict[str, str]] = {
    "bridge_outage": {
        "headline": "The bridge is out",
        "impact": "Whole delivery is completely fucked.",
    },
    "city_center_accident": {
        "headline": "Massive accident in the city center",
        "impact": "Delivery is fucked.",
    },
    "city_center_protest": {
        "headline": "Protest in the city center",
        "impact": "Drivers can't get through — deliveries backing up.",
    },
    "tomato_supply_shock": {
        "headline": "Farmers are protesting — no tomatoes delivered",
        "impact": "Orders need to be fixed, adjusted, or refunded.",
    },
}

EVENT_LABELS: dict[str, str] = {
    "route_blocked": "Route blocked — delivery can't get through",
    "driver_rerouted": "Driver rerouted — longer detour",
    "order_at_risk": "Order at risk — may miss delivery window",
    "refund_spike": "Refund spike — customers asking for money back",
    "inventory_outage": "Out of stock — menu item needs substitution",
}

STREAM_NOTIFICATION_LABELS: dict[str, str] = {
    "order_created": "New order — customer waiting for delivery",
    "gk_started": "Kitchen started prepping",
    "driver_ping": "Driver on the move",
    "order_cancelled": "Order cancelled",
    "order_delivered": "Order delivered",
}

INDEX_HTML = Path(__file__).parent.parent / "index.html"
ws = WorkspaceClient()

_DEMO_STATE_CACHE: dict[str, Any] = {"expires": 0.0, "payload": None}
_DEMO_STATE_TTL_S = int(os.environ.get("DEMO_STATE_CACHE_SECONDS", "12"))


class ActivateScenario(BaseModel):
    scenario_id: str
    seed: int


class ActionRequest(BaseModel):
    action_type: str
    order_id: str = ""
    incident_id: str = ""
    notes: str = ""


# ── Simulation lifecycle events (written to Lakebase) ────────────────────────
# The map/simulation runs client-side; the frontend POSTs these discrete
# transitions (not per-second ticks) so Lakebase holds a real backend of every
# order, status change, refund and complaint.

class OrderEvent(BaseModel):
    order_id: str
    session_id: str = ""
    city: str = ""
    kitchen: str = ""
    vehicle: str = ""
    kind: str = ""
    cold: bool = False
    cross_river: bool = False
    status: str = "placed"
    late_min: int = 0
    max_delay_min: int = 0
    placed_at: float | None = None       # epoch seconds or millis
    promised_at: float | None = None


class StatusEvent(BaseModel):
    order_id: str
    status: str
    late_min: int = 0


class RefundEvent(BaseModel):
    order_id: str
    reason: str = ""
    amount: float | None = None


class ComplaintEvent(BaseModel):
    order_id: str
    quote: str = ""
    resolution: str | None = None


class ConfigBody(BaseModel):
    city: str = ""
    orders: int = 45
    speed: int = 4


class SessionBody(BaseModel):
    session: dict[str, Any]


class AgentChat(BaseModel):
    message: str
    history: list[dict[str, str]] = []


# Last-used demo config. Persisted to Lakebase with an in-process fallback so a
# refresh can skip the setup screen when localStorage is blocked/partitioned.
_LAST_CONFIG: dict[str, Any] | None = None
_LAST_ORDERS_MIRROR_AT = 0.0


def _sql_str(value: str) -> str:
    return (value or "").replace("'", "''")


def _mirror_active_city_to_uc(city_id: str) -> None:
    """Best-effort: warehouse SQL reads demo_active_city for the current picker."""
    if not WAREHOUSE_ID or not city_id:
        return
    city = _sql_str(city_id.strip().lower())
    try:
        _query(
            f"""
            MERGE INTO {CATALOG}.{SIMULATOR_SCHEMA}.demo_active_city AS t
            USING (SELECT 1 AS id, '{city}' AS city_id) AS s
            ON t.id = s.id
            WHEN MATCHED THEN UPDATE SET
              city_id = s.city_id,
              updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (id, city_id, updated_at)
              VALUES (s.id, s.city_id, current_timestamp())
            """
        )
    except HTTPException as e:
        log.warning(f"demo_active_city UC mirror skipped: {e.detail}")
    except Exception as e:
        log.warning(f"demo_active_city UC mirror skipped: {e}")


def _mirror_live_orders_to_uc(*, force: bool = False) -> None:
    """Best-effort: warehouse query 3 reads catastrophe_live_orders on Lakehouse."""
    global _LAST_ORDERS_MIRROR_AT
    if not WAREHOUSE_ID:
        return
    now = time.time()
    if not force and now - _LAST_ORDERS_MIRROR_AT < 5:
        return
    rows = db.orders_for_warehouse_mirror()
    if not rows:
        log.warning("catastrophe_live_orders UC mirror skipped: no Lakebase orders to mirror")
        return
    vals: list[str] = []
    for r in rows:
        oid = _sql_str(str(r.get("order_id", "")))
        sid = _sql_str(str(r.get("session_id") or ""))
        city = _sql_str(str(r.get("city") or ""))
        st = _sql_str(str(r.get("status") or ""))
        kind = _sql_str(str(r.get("kind") or ""))
        late = int(r.get("late_min") or 0)
        ts = r.get("updated_at")
        if hasattr(ts, "isoformat"):
            ts_lit = f"timestamp '{ts.isoformat().replace('T', ' ')[:19]}'"
        else:
            ts_lit = "current_timestamp()"
        vals.append(f"('{oid}', '{sid}', '{city}', '{st}', '{kind}', {late}, {ts_lit})")
    try:
        _query(
            f"""
            CREATE OR REPLACE TABLE {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_live_orders AS
            SELECT * FROM VALUES {", ".join(vals)}
            AS t(order_id, session_id, city, status, kind, late_min, updated_at)
            """
        )
        _LAST_ORDERS_MIRROR_AT = now
    except HTTPException as e:
        log.warning(f"catastrophe_live_orders UC mirror skipped: {e.detail}")
    except Exception as e:
        log.warning(f"catastrophe_live_orders UC mirror skipped: {e}")


ACTION_LABELS: dict[str, str] = {
    "reroute_driver": "Reroute driver",
    "issue_credit": "Issue customer credit",
    "cancel_order": "Cancel order",
    "acknowledge_incident": "Acknowledge incident",
}


def _await_statement(statement_id: str, timeout_s: int = 40):
    deadline = time.time() + timeout_s
    response = ws.statement_execution.get_statement(statement_id)
    while True:
        status = getattr(response, "status", None)
        state = getattr(getattr(status, "state", None), "value", "")
        if state in ("SUCCEEDED", "FAILED", "CANCELED", "CLOSED"):
            return response
        if time.time() > deadline:
            raise HTTPException(status_code=504, detail="SQL statement timeout")
        time.sleep(1)
        response = ws.statement_execution.get_statement(statement_id)


def _column_names(response: Any) -> list[str]:
    """Extract result column names from a statement response (manifest location varies)."""
    for manifest in (
        getattr(response, "manifest", None),
        getattr(getattr(response, "result", None), "manifest", None),
    ):
        if not manifest:
            continue
        schema = getattr(manifest, "schema", None)
        columns = getattr(schema, "columns", None) if schema else None
        if columns:
            names = [getattr(c, "name", None) for c in columns]
            return [n for n in names if n]
    return []


def _statement_rows(response: Any) -> list[dict[str, Any]]:
    result = getattr(response, "result", None)
    data = getattr(result, "data_array", None) if result else None
    names = _column_names(response)
    if not data or not names:
        return []
    out: list[dict[str, Any]] = []
    for row in data:
        out.append({names[i]: row[i] for i in range(min(len(names), len(row)))})
    return out


def _query(statement: str) -> list[dict[str, Any]]:
    if not WAREHOUSE_ID:
        raise HTTPException(status_code=503, detail="DATABRICKS_WAREHOUSE_ID is not configured")

    initial = ws.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        wait_timeout="30s",
        on_wait_timeout=ExecuteStatementRequestOnWaitTimeout.CONTINUE,
        disposition=Disposition.INLINE,
        format=Format.JSON_ARRAY,
    )
    statement_id = getattr(initial, "statement_id", None)
    response = _await_statement(statement_id) if statement_id else initial

    status = getattr(response, "status", None)
    state = getattr(getattr(status, "state", None), "value", "")
    if state != "SUCCEEDED":
        error_obj = getattr(status, "error", None)
        detail = str(error_obj) if error_obj else f"SQL statement ended in state={state}"
        raise HTTPException(status_code=500, detail=detail)

    return _statement_rows(response)


def _exec(statement: str) -> None:
    _query(statement)


def _theater() -> dict[str, Any]:
    return THEATER_CONFIG.get(DEMO_THEATER, THEATER_CONFIG["amsterdam"])


def _location_filter(column: str = "location_id") -> str:
    ids = _theater().get("location_ids") or []
    if not ids:
        return ""
    joined = ", ".join(str(int(i)) for i in ids)
    return f" AND {column} IN ({joined})"


def _scenario_filter(column: str = "scenario_id") -> str:
    safe = DEFAULT_SCENARIO.replace("'", "''")
    return f" AND {column} = '{safe}'"


def _label_event(event_type: str) -> str:
    return EVENT_LABELS.get(event_type, event_type.replace("_", " ").title())


def _enrich_incidents(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    for row in rows:
        row["event_label"] = _label_event(str(row.get("event_type", "")))
    return rows


def _scenario_brief() -> dict[str, str]:
    return SCENARIO_BRIEF.get(DEFAULT_SCENARIO, SCENARIO_BRIEF["bridge_outage"])


def _bbox_filter_lat_lon(lat_col: str, lon_col: str) -> str:
    bbox = _theater().get("bbox")
    if not bbox:
        return ""
    min_lat, min_lon, max_lat, max_lon = bbox
    return f" AND {lat_col} BETWEEN {min_lat} AND {max_lat} AND {lon_col} BETWEEN {min_lon} AND {max_lon}"


def _dedupe_points(
    rows: list[dict[str, Any]],
    lat_key: str,
    lon_key: str,
    *,
    precision: int = 3,
    limit: int = 24,
) -> list[dict[str, Any]]:
    """Keep one point per map cell so markers don't stack on identical coordinates."""
    seen: set[tuple[float, float]] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        if row.get(lat_key) is None or row.get(lon_key) is None:
            continue
        try:
            lat = round(float(row[lat_key]), precision)
            lon = round(float(row[lon_key]), precision)
        except (TypeError, ValueError):
            continue
        key = (lat, lon)
        if key in seen:
            continue
        seen.add(key)
        out.append(row)
        if len(out) >= limit:
            break
    return out


def _summarize_events_for_map(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One map marker per kitchen — incidents share kitchen coordinates."""
    grouped: dict[str, dict[str, Any]] = {}
    for event in events:
        loc = str(event.get("location_name") or "Kitchen")
        lat = event.get("latitude")
        lon = event.get("longitude")
        bucket = grouped.setdefault(
            loc,
            {
                "location_name": loc,
                "latitude": lat,
                "longitude": lon,
                "signal_count": 0,
                "samples": [],
            },
        )
        bucket["signal_count"] += 1
        label = event.get("event_label") or _label_event(str(event.get("event_type", "")))
        if len(bucket["samples"]) < 3:
            bucket["samples"].append(label)
    return list(grouped.values())


def _build_notifications(
    incidents: list[dict[str, Any]],
    *,
    action_rows: list[dict[str, Any]] | None = None,
    stream_rows: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    notes: list[dict[str, Any]] = []

    for row in action_rows or []:
        label = ACTION_LABELS.get(str(row.get("action_type", "")), str(row.get("action_type", "")))
        target = row.get("order_id") or row.get("incident_id") or ""
        notes.append(
            {
                "kind": "action",
                "text": label,
                "detail": target,
                "order_id": row.get("order_id") or "",
                "incident_id": row.get("incident_id") or "",
            }
        )

    for inc in incidents:
        notes.append(
            {
                "kind": "crisis",
                "text": inc.get("event_label") or _label_event(str(inc.get("event_type", ""))),
                "detail": f"{inc.get('location_name', 'Kitchen')} · {inc.get('affected_orders', 0)} orders · +{inc.get('expected_delay_min', 0)} min",
                "order_id": "",
                "incident_id": inc.get("incident_id") or "",
            }
        )

    rows = stream_rows or []
    for row in rows:
        et = str(row.get("event_type", ""))
        oid = str(row.get("order_id") or "")
        notes.append(
            {
                "kind": "stream",
                "text": STREAM_NOTIFICATION_LABELS.get(et, et.replace("_", " ")),
                "detail": f"Order {oid}" if oid else "",
                "order_id": oid,
                "incident_id": "",
            }
        )
    return notes


def _pillars() -> dict[str, Any]:
    """Three ways of solving the catastrophe — wording from the DevConnect brief."""
    return {
        "what_to_use": {
            "title": "What to use",
            "approach": "The artisanal approach",
            "items": [
                "Reyden",
                "Lakehouse / real-time",
                "Lakebase",
                "Managed table features — DR, Iceberg interop, catalog commits",
            ],
        },
        "how_to_use": {
            "title": "How to use",
            "approach": "The tokenmaxxing approach",
            "items": [
                "MCP integration",
                "Zero Ops",
                "Omnigent",
                "AI Gateway — manage costs",
            ],
        },
        "how_to_share": {
            "title": "How to share",
            "approach": "Give data to the masses",
            "items": [
                "App Builder",
                "Spaces",
                "Custom AI/BI",
                "Genie agents with Slack integrations",
            ],
        },
        "cta": (
            "Try these features today — on Free Edition if you don't have "
            "a usable Enterprise subscription."
        ),
    }


# NOTE: async on purpose. FastAPI runs *sync* (`def`) routes in a bounded
# threadpool (default 40). The sim fires many rapid POSTs and the agent's chat
# endpoint blocks a thread for the full LLM+warehouse duration (10-30s), so under
# load the threadpool saturates. If the page route were sync too, a browser
# reload would queue behind those busy threads and hang (header spinner never
# resolves). Serving the page from the event loop keeps reloads instant no matter
# how busy the threadpool is.
@app.get("/", include_in_schema=False)
async def index() -> FileResponse:
    if not INDEX_HTML.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(INDEX_HTML), headers={"Cache-Control": "no-store, no-cache, must-revalidate, max-age=0"})


@app.get("/api/health")
def health() -> dict[str, Any]:
    theater = _theater()
    return {
        "status": "ok",
        "catalog": CATALOG,
        "scenario_id": DEFAULT_SCENARIO,
        "scenario_seed": CATASTROPHE_SEED,
        "theater_label": theater["label"],
        "map_center": theater["center"],
        "map_zoom": theater["zoom"],
    }


@app.get("/api/demo-state")
def demo_state(refresh: bool = Query(default=False)) -> dict[str, Any]:
    """DevConnect demo — shaped around the original brief."""
    now = time.time()
    if not refresh and _DEMO_STATE_CACHE["payload"] is not None and now < _DEMO_STATE_CACHE["expires"]:
        return _DEMO_STATE_CACHE["payload"]

    theater = _theater()
    brief = _scenario_brief()
    zone = SCENARIO_ZONES.get(DEFAULT_SCENARIO, SCENARIO_ZONES["bridge_outage"])

    kitchens_sql = f"""
        SELECT l.location_id, l.name, l.lat AS latitude, l.lon AS longitude
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.locations l
        WHERE 1=1{_location_filter("l.location_id")}
        ORDER BY l.location_id
    """
    events_sql = f"""
        SELECT i.incident_id, i.event_type, i.severity, i.affected_orders,
               i.expected_delay_min, l.name AS location_name,
               l.lat AS latitude, l.lon AS longitude
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_incidents i
        LEFT JOIN {CATALOG}.{SIMULATOR_SCHEMA}.locations l ON i.location_id = l.location_id
        WHERE 1=1{_location_filter("i.location_id")}{_scenario_filter("i.scenario_id")}
        ORDER BY i.impact_score DESC
    """
    actions_sql = f"""
        SELECT action_type, order_id, incident_id, notes, created_at_utc
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_actions
        ORDER BY created_at_utc DESC
        LIMIT 15
    """

    with ThreadPoolExecutor(max_workers=3) as pool:
        f_kitchens = pool.submit(_query, kitchens_sql)
        f_events = pool.submit(_query, events_sql)
        f_actions = pool.submit(_query, actions_sql)
        kitchens = f_kitchens.result()
        events = _enrich_incidents(f_events.result())
        action_rows = f_actions.result()

    drivers_raw: list[dict[str, Any]] = []
    orders_raw: list[dict[str, Any]] = []
    stream_rows: list[dict[str, Any]] = []

    drivers = _dedupe_points(drivers_raw, "lat", "lon", precision=3, limit=20)
    orders = _dedupe_points(orders_raw, "lat", "lon", precision=3, limit=20)
    event_summaries = _summarize_events_for_map(events)
    notifications = _build_notifications(events, action_rows=action_rows, stream_rows=stream_rows)
    sample_order_ids = []
    for row in orders_raw:
        oid = str(row.get("order_id") or "").strip()
        if oid and oid not in sample_order_ids:
            sample_order_ids.append(oid)
        if len(sample_order_ids) >= 6:
            break

    payload = {
        "brief": brief,
        "configured_scenario": DEFAULT_SCENARIO,
        "configured_seed": CATASTROPHE_SEED,
        "configuration_note": (
            "Scenario is configuration: set CATASTROPHE_SCENARIO and CATASTROPHE_SEED "
            "on the devconnect job, then rerun."
        ),
        "theater": theater,
        "zone": zone,
        "map": {
            "kitchens": kitchens,
            "incidents": events,
            "event_summaries": event_summaries,
            "drivers": drivers,
            "orders": orders,
            "counts": {
                "orders_total": len(orders_raw),
                "orders_on_map": len(orders),
                "drivers_total": len(drivers_raw),
                "drivers_on_map": len(drivers),
                "crisis_signals": len(events),
            },
        },
        "notifications": notifications,
        "sample_order_ids": sample_order_ids,
    }
    _DEMO_STATE_CACHE["payload"] = payload
    _DEMO_STATE_CACHE["expires"] = now + _DEMO_STATE_TTL_S
    return payload


@app.get("/api/cities")
def cities_api() -> list[dict[str, Any]]:
    """Selectable cities for the config screen (registry order, active first)."""
    active = CITY if CITY in _CITIES else "amsterdam"
    ordered = [active] + [cid for cid in _CITIES if cid != active]
    return [{"id": cid, "name": _CITIES[cid]["name"], "flag": _CITIES[cid]["flag"],
             "active": cid == active} for cid in ordered]


@app.get("/api/city")
def city_api(id: str = Query(default="")) -> dict[str, Any]:
    """City geography (bridge choke, reroute, banks, regions) for the map. With
    no id, returns the deploy-time active city; with an id, computes that city."""
    cid = id.strip().lower()
    if cid and cid in _CITIES:
        return _city_config_for(cid)
    return _city_config()


@app.get("/api/kitchens")
def kitchens_api(city: str = Query(default="")) -> list[dict[str, Any]]:
    """Ghost-kitchen locations for a city from the catastrophe_kitchens table.
    With no `city`, uses the deploy-time active city. Falls back to a generated
    spread if the table has no rows for the requested city (e.g. the stage was
    last materialized for a different city)."""
    cid = city.strip().lower()
    city_name = _CITIES[cid]["name"] if cid and cid in _CITIES else CITY_NAME
    rows = _query(
        f"""
        SELECT kitchen_id, name, neighborhood, city, location_id, lat, lon, address
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_kitchens
        WHERE city = '{_sql_str(city_name)}'
        ORDER BY kitchen_id
        """
    )
    if not rows and cid and cid in _CITIES:
        return _generate_city_kitchens(cid)
    return rows


@app.get("/api/scenarios")
def scenarios() -> list[dict[str, Any]]:
    return _query(
        f"""
        SELECT scenario_id, title, description, severity, eta_minutes, blast_radius_km,
               refund_pressure, delivery_delay_multiplier, inventory_shock
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_scenarios
        ORDER BY CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2 ELSE 3 END, scenario_id
        """
    )


@app.get("/api/incidents")
def incidents(limit: int = Query(default=80, ge=1, le=500)) -> list[dict[str, Any]]:
    return _enrich_incidents(
        _query(
            f"""
        SELECT i.incident_id, i.scenario_id, i.location_id, i.created_at_utc, i.event_type, i.severity,
               i.impact_score, i.expected_delay_min, i.affected_orders, i.requires_operator_action,
               l.name AS location_name, l.name AS city,
               l.lat AS latitude, l.lon AS longitude
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_incidents i
        LEFT JOIN {CATALOG}.{SIMULATOR_SCHEMA}.locations l ON i.location_id = l.location_id
        WHERE 1=1{_location_filter("i.location_id")}{_scenario_filter("i.scenario_id")}
        ORDER BY i.created_at_utc DESC
        LIMIT {int(limit)}
        """
        )
    )


@app.get("/api/map-layers")
def map_layers() -> dict[str, Any]:
    locations = _query(
        f"""
        WITH risk AS (
          SELECT location_id,
                 MAX(impact_score) AS max_impact_score,
                 SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) AS critical_events
          FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_incidents
          WHERE 1=1{_location_filter("location_id")}
          GROUP BY location_id
        )
        SELECT l.location_id, l.name, l.name AS city, true AS is_active,
               l.lat AS latitude, l.lon AS longitude,
               0 AS orders_created,
               0 AS orders_delivered,
               0 AS orders_cancelled,
               COALESCE(r.max_impact_score, 0.0) AS risk_score,
               COALESCE(r.critical_events, 0) AS critical_events
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.locations l
        LEFT JOIN risk r ON l.location_id = r.location_id
        WHERE 1=1{_location_filter("l.location_id")}
        """
    )
    incidents_layer = incidents(limit=250)
    return {
        "locations": locations,
        "incidents": incidents_layer,
        "drivers": [],
        "zone": SCENARIO_ZONES.get(DEFAULT_SCENARIO, SCENARIO_ZONES["bridge_outage"]),
        "scenario_id": DEFAULT_SCENARIO,
    }


@app.get("/api/notification-rail")
def notification_rail(limit: int = Query(default=40, ge=1, le=300)) -> list[dict[str, Any]]:
    return _query(
        f"""
        WITH n AS (
          SELECT i.incident_id, i.scenario_id, i.event_type, i.severity, i.location_id,
                 i.expected_delay_min, i.affected_orders, i.impact_score, i.created_at_utc,
                 l.name AS location_name, l.name AS city
          FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_incidents i
          LEFT JOIN {CATALOG}.{SIMULATOR_SCHEMA}.locations l ON i.location_id = l.location_id
          WHERE i.requires_operator_action = true{_location_filter("i.location_id")}
        )
        SELECT *,
               CASE
                 WHEN severity='critical' OR impact_score >= 0.85 THEN 'P1'
                 WHEN severity='high' OR impact_score >= 0.65 THEN 'P2'
                 ELSE 'P3'
               END AS priority
        FROM n
        ORDER BY impact_score DESC, created_at_utc DESC
        LIMIT {int(limit)}
        """
    )


@app.get("/api/command-overview")
def command_overview() -> dict[str, Any]:
    metrics = _query(
        f"""
        SELECT
          0 AS created_orders,
          0 AS delivered_orders,
          0 AS cancelled_orders,
          COUNT(*) AS total_incidents,
          SUM(CASE WHEN severity='critical' THEN 1 ELSE 0 END) AS critical_incidents,
          AVG(impact_score) AS avg_impact,
          SUM(CASE WHEN requires_operator_action THEN 1 ELSE 0 END) AS action_required
        FROM {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_incidents
        WHERE 1=1{_location_filter("location_id")}
        """
    )
    row = metrics[0] if metrics else {}
    if db.enabled():
        by_status = {str(r.get("status", "")): int(r.get("n") or 0) for r in db.summary().get("by_status", [])}
        row = {
            **row,
            "created_orders": sum(by_status.values()),
            "delivered_orders": by_status.get("delivered", 0),
            "cancelled_orders": by_status.get("cancelled", 0),
        }
    return row


@app.get("/api/role-brief")
def role_brief(role: str = Query(default="commander")) -> dict[str, Any]:
    normalized = role.strip().lower()
    briefs = {
        "commander": {
            "title": "City Command Brief",
            "focus": ["P1 incidents", "delivery continuity", "cross-team escalation"],
            "actions": ["Dispatch reroute protocol", "Authorize proactive credits", "Broadcast stakeholder update"],
        },
        "ops": {
            "title": "Operations Brief",
            "focus": ["driver reroutes", "backlog by location", "delay hotspots"],
            "actions": ["Rebalance drivers", "Pause low-margin routes", "Open temporary prep node"],
        },
        "support": {
            "title": "Support Brief",
            "focus": ["high-risk orders", "complaint volume", "refund candidate queue"],
            "actions": ["Push response templates", "Queue priority callbacks", "Escalate repeat offenders"],
        },
        "executive": {
            "title": "Executive Brief",
            "focus": ["business impact", "customer impact", "time-to-recovery"],
            "actions": ["Approve recovery spend", "Set public comms stance", "Review AI gateway spend guardrails"],
        },
    }
    return briefs.get(normalized, briefs["commander"])


@app.get("/api/orders/{order_id}")
def order_detail(order_id: str) -> dict[str, Any]:
    safe_id = order_id.strip()
    if not safe_id:
        raise HTTPException(status_code=400, detail="order_id required")

    if db.enabled():
        order = db.get_order(safe_id)
        events = db.order_timeline(safe_id)
        return {
            "order_id": order_id,
            "order": order or {},
            "events": events,
            "driver": {},
            "source": "lakebase",
        }

    return {"order_id": order_id, "order": {}, "events": [], "driver": {}, "source": "none"}


@app.post("/api/actions/execute")
def execute_action(body: ActionRequest) -> dict[str, Any]:
    if body.action_type not in ACTION_LABELS:
        raise HTTPException(status_code=400, detail=f"Unknown action_type: {body.action_type}")
    if not body.order_id and not body.incident_id:
        raise HTTPException(status_code=400, detail="order_id or incident_id required")

    action_id = str(uuid.uuid4())
    now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    _exec(
        f"""
        INSERT INTO {CATALOG}.{SIMULATOR_SCHEMA}.catastrophe_actions
        (action_id, action_type, order_id, incident_id, notes, created_at_utc)
        VALUES (
          '{_sql_str(action_id)}',
          '{_sql_str(body.action_type)}',
          '{_sql_str(body.order_id)}',
          '{_sql_str(body.incident_id)}',
          '{_sql_str(body.notes)}',
          TIMESTAMP '{now}'
        )
        """
    )
    # Mirror to Lakebase so the operator actions live in the same backend as
    # the orders/statuses/refunds they act on.
    db.add_action(action_id, body.action_type, body.order_id, body.incident_id, body.notes)
    _DEMO_STATE_CACHE["expires"] = 0.0
    label = ACTION_LABELS[body.action_type]
    target = body.order_id or body.incident_id
    return {
        "ok": True,
        "action_id": action_id,
        "message": f"{label} recorded for {target}",
    }


@app.post("/api/sim/order")
def sim_order(body: OrderEvent) -> dict[str, Any]:
    db.upsert_order(body.model_dump())
    return {"ok": True, "persisted": db.enabled()}


@app.post("/api/sim/status")
def sim_status(body: StatusEvent) -> dict[str, Any]:
    db.add_status(body.order_id, body.status, body.late_min)
    return {"ok": True, "persisted": db.enabled()}


@app.post("/api/sim/refund")
def sim_refund(body: RefundEvent) -> dict[str, Any]:
    db.add_refund(body.order_id, body.reason, body.amount)
    return {"ok": True, "persisted": db.enabled()}


@app.post("/api/sim/complaint")
def sim_complaint(body: ComplaintEvent) -> dict[str, Any]:
    if body.resolution:
        db.resolve_complaint(body.order_id, body.resolution)
    else:
        db.add_complaint(body.order_id, body.quote)
    return {"ok": True, "persisted": db.enabled()}


@app.get("/api/sim/orders")
def sim_orders(limit: int = Query(200, ge=1, le=1000)) -> dict[str, Any]:
    return {"enabled": db.enabled(), "orders": db.recent_orders(limit)}


@app.get("/api/sim/route-policy")
def sim_route_policy(city: str = Query("amsterdam")) -> dict[str, Any]:
    return {"enabled": db.enabled(), "policy": db.get_route_policy(city.strip().lower())}


@app.get("/api/sim/order-statuses")
def sim_order_statuses(limit: int = Query(1000, ge=1, le=5000)) -> dict[str, Any]:
    """Lightweight status feed the map polls so SQL run directly on Lakebase
    (e.g. flipping stuck orders to 'rerouted' or 'reordered') is reflected live
    on screen."""
    _mirror_live_orders_to_uc()
    return {"enabled": db.enabled(), "statuses": db.order_statuses(limit)}


@app.get("/api/sim/refund-summary")
def refund_summary(session_id: str = Query(default="")) -> dict[str, Any]:
    """Lightweight refund totals for the stats panel (no row list)."""
    sid = session_id.strip() or None
    return {
        "enabled": db.enabled(),
        "summary": db.session_refund_summary(sid),
    }


@app.get("/api/sim/refunds")
def sim_refunds(
    limit: int = Query(500, ge=1, le=2000),
    session_id: str = Query(default=""),
) -> dict[str, Any]:
    """Recent refunds feed the map polls so a refund issued directly on Lakebase
    surfaces as a 'refund sent' notification on screen."""
    sid = session_id.strip() or None
    return {
        "enabled": db.enabled(),
        "refunds": db.recent_refunds(limit),
        "session_refunds": db.session_refunds(sid, limit),
        "summary": db.session_refund_summary(sid),
    }


@app.get("/api/sim/summary")
def sim_summary() -> dict[str, Any]:
    return db.summary()


@app.get("/api/config")
def get_config() -> dict[str, Any]:
    global _LAST_CONFIG
    if _LAST_CONFIG is None:
        _LAST_CONFIG = db.get_config()
    return {"config": _LAST_CONFIG}


@app.post("/api/config")
def set_config(body: ConfigBody) -> dict[str, Any]:
    global _LAST_CONFIG
    city = body.city.strip().lower()
    _LAST_CONFIG = {"city": city, "orders": body.orders, "speed": body.speed}
    db.set_config(city, body.orders, body.speed)
    db.ensure_route_policy_open(city)
    _mirror_active_city_to_uc(city)
    return {"ok": True, "config": _LAST_CONFIG}


@app.get("/api/session")
def get_session() -> dict[str, Any]:
    return {"session": db.get_session()}


@app.post("/api/session")
def set_session(body: SessionBody) -> dict[str, Any]:
    db.set_session(body.session)
    return {"ok": True, "persisted": db.enabled()}


@app.delete("/api/session")
def delete_session() -> dict[str, Any]:
    db.clear_session()
    return {"ok": True}


@app.post("/api/scenarios/activate")
def activate_scenario(body: ActivateScenario) -> dict[str, Any]:
    return {
        "accepted": True,
        "scenario_id": body.scenario_id,
        "seed": body.seed,
        "note": "Re-run Catastrophe_Command stage with matching parameters to materialize incidents.",
    }


@app.get("/api/solution-paths")
def solution_paths() -> dict[str, Any]:
    return _pillars()


@app.get("/api/share-pack")
def share_pack() -> dict[str, str]:
    return {
        "app_builder_url": APP_BUILDER_URL,
        "genie_space_url": GENIE_SPACE_URL,
        "custom_dashboard_url": CUSTOM_DASHBOARD_URL,
        "slack_channel_hint": SLACK_CHANNEL_HINT,
    }


# ── Catastrophe agent (Act 2) ────────────────────────────────────────────────
# The agent is a single, swappable file (app/agent.py). It owns the LLM and the
# vetted query catalog; the app injects the two SQL executors so the agent stays
# decoupled from FastAPI/psycopg: `_query` for the UC SQL warehouse (Q3–Q5b) and
# `db.run_script` for Lakebase (Q1–Q2).
_AGENT: Agent | None = None


def _agent_gateway_endpoint() -> str:
    return (AI_GATEWAY_ENDPOINT_NAME or f"{CATALOG}.default.command-agent").strip()


def _agent() -> Agent:
    global _AGENT
    if _AGENT is None:
        _AGENT = Agent(
            warehouse_exec=_query,
            lakebase_exec=db.run_script,
            catalog=CATALOG,
            gateway_endpoint=_agent_gateway_endpoint(),
        )
    return _AGENT


@app.get("/api/agent/info")
def agent_info() -> dict[str, Any]:
    agent = _agent()
    return {
        "available": agent.available(),
        "model": agent.model,
        "via_gateway": agent.via_gateway,
    }


@app.post("/api/agent/chat")
async def agent_chat(body: AgentChat) -> dict[str, Any]:
    message = (body.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message required")
    if not _agent().available():
        raise HTTPException(
            status_code=503,
            detail="Agent unavailable — workspace auth could not be resolved.",
        )
    # The agent's `warehouse` actions (revenue_at_risk / today_vs_normal /
    # ingredient checks) read the UC mirror of the live Lakebase state. That
    # mirror is normally refreshed by the map polling /api/sim/order-statuses,
    # so if the operator talks to the agent while the map isn't actively polling
    # the queries see a stale/empty table. Refresh it here (best-effort) so the
    # agent always reads current data. Throttled + no-op on empty inside.
    try:
        cfg = _LAST_CONFIG or db.get_config()
        if cfg and cfg.get("city"):
            _mirror_active_city_to_uc(str(cfg["city"]))
        _mirror_live_orders_to_uc(force=True)
    except Exception as e:  # noqa: BLE001
        log.warning(f"Pre-agent UC mirror refresh skipped: {e}")
    try:
        # Run off FastAPI's sync-route threadpool. The sim fires dozens of
        # blocking POSTs during the catastrophe; a sync agent_chat would queue
        # behind them and the Apps proxy would drop the connection → browser
        # shows "Could not reach the agent."
        return await asyncio.to_thread(_agent().run, message, body.history)
    except HTTPException:
        raise
    except Exception as e:
        log.warning(f"Agent chat failed: {type(e).__name__}: {e}")
        raise HTTPException(status_code=502, detail=f"Agent error: {e}")
