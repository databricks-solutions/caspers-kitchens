"""Deterministic catastrophe scenarios for the DevConnect command demo."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import math
import random
from typing import Dict, List, Tuple


@dataclass(frozen=True)
class Scenario:
    id: str
    title: str
    description: str
    severity: str
    eta_minutes: int
    blast_radius_km: float
    refund_pressure: float
    delivery_delay_multiplier: float
    inventory_shock: float


SCENARIOS: Dict[str, Scenario] = {
    "bridge_outage": Scenario(
        id="bridge_outage",
        title="Bridge Failure",
        description="Main bridge closure breaks east-west delivery routes.",
        severity="critical",
        eta_minutes=210,
        blast_radius_km=7.0,
        refund_pressure=0.88,
        delivery_delay_multiplier=2.4,
        inventory_shock=0.20,
    ),
    "city_center_accident": Scenario(
        id="city_center_accident",
        title="City Center Accident",
        description="Multi-vehicle crash blocks downtown arteries.",
        severity="high",
        eta_minutes=140,
        blast_radius_km=4.8,
        refund_pressure=0.61,
        delivery_delay_multiplier=1.8,
        inventory_shock=0.07,
    ),
    "city_center_protest": Scenario(
        id="city_center_protest",
        title="City Protest",
        description="Large protest and police cordons reroute drivers.",
        severity="high",
        eta_minutes=180,
        blast_radius_km=5.4,
        refund_pressure=0.53,
        delivery_delay_multiplier=1.7,
        inventory_shock=0.05,
    ),
    "tomato_supply_shock": Scenario(
        id="tomato_supply_shock",
        title="Tomato Supply Shock",
        description="Farmers protest halts tomato deliveries, menu substitutions needed.",
        severity="critical",
        eta_minutes=300,
        blast_radius_km=12.0,
        refund_pressure=0.72,
        delivery_delay_multiplier=1.4,
        inventory_shock=0.85,
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# Casper's Kitchens — real Amsterdam ghost-kitchen locations for the DevConnect
# catastrophe demo. Coordinates are real neighbourhood points near the Amstel so
# the Berlagebrug closure is a believable choke point. Linked to the Amsterdam
# row in simulator.locations (location_id 7).
# ─────────────────────────────────────────────────────────────────────────────
AMSTERDAM_LOCATION_ID = 7

AMSTERDAM_KITCHENS: List[dict] = [
    {"kitchen_id": "ams-depijp",     "name": "Casper's De Pijp",         "neighborhood": "De Pijp",          "lat": 52.35450, "lon": 4.89180, "address": "Ferdinand Bolstraat 100, 1072 LR Amsterdam"},
    {"kitchen_id": "ams-rivieren",   "name": "Casper's Rivierenbuurt",   "neighborhood": "Rivierenbuurt",    "lat": 52.34700, "lon": 4.90500, "address": "Rijnstraat 50, 1079 GX Amsterdam"},
    {"kitchen_id": "ams-amsteldijk", "name": "Casper's Amsteldijk",      "neighborhood": "Amsteldijk",       "lat": 52.35200, "lon": 4.90100, "address": "Amsteldijk 120, 1078 RT Amsterdam"},
    {"kitchen_id": "ams-watergraaf", "name": "Casper's Watergraafsmeer", "neighborhood": "Watergraafsmeer",  "lat": 52.35120, "lon": 4.93000, "address": "Middenweg 40, 1097 BN Amsterdam"},
    {"kitchen_id": "ams-oost",       "name": "Casper's Oost",            "neighborhood": "Oosterparkbuurt",  "lat": 52.35950, "lon": 4.92000, "address": "Linnaeusstraat 80, 1093 EN Amsterdam"},
    {"kitchen_id": "ams-amstelkw",   "name": "Casper's Amstelkwartier",  "neighborhood": "Amstelkwartier",   "lat": 52.34650, "lon": 4.91750, "address": "Spaklerweg 20, 1096 BA Amsterdam"},
    {"kitchen_id": "ams-jordaan",    "name": "Casper's Jordaan",         "neighborhood": "Jordaan",          "lat": 52.37400, "lon": 4.88300, "address": "Rozengracht 60, 1016 NC Amsterdam"},
    {"kitchen_id": "ams-nieuwmarkt", "name": "Casper's Nieuwmarkt",      "neighborhood": "Nieuwmarkt",       "lat": 52.37200, "lon": 4.90100, "address": "Kloveniersburgwal 20, 1012 CT Amsterdam"},
    {"kitchen_id": "ams-oudwest",    "name": "Casper's Oud-West",        "neighborhood": "Oud-West",         "lat": 52.36600, "lon": 4.87200, "address": "Kinkerstraat 150, 1053 EK Amsterdam"},
    {"kitchen_id": "ams-westerpark", "name": "Casper's Westerpark",      "neighborhood": "Westerpark",       "lat": 52.38600, "lon": 4.87700, "address": "Haarlemmerweg 8, 1014 BE Amsterdam"},
    {"kitchen_id": "ams-boslommer",  "name": "Casper's Bos en Lommer",   "neighborhood": "Bos en Lommer",    "lat": 52.37700, "lon": 4.85600, "address": "Bos en Lommerweg 200, 1055 EA Amsterdam"},
    {"kitchen_id": "ams-baarsjes",   "name": "Casper's De Baarsjes",     "neighborhood": "De Baarsjes",      "lat": 52.36900, "lon": 4.85900, "address": "Jan Evertsenstraat 90, 1056 EG Amsterdam"},
    {"kitchen_id": "ams-museum",     "name": "Casper's Museumkwartier",  "neighborhood": "Museumkwartier",   "lat": 52.35800, "lon": 4.88100, "address": "Van Baerlestraat 30, 1071 AR Amsterdam"},
    {"kitchen_id": "ams-weesper",    "name": "Casper's Weesperzijde",    "neighborhood": "Weesperzijde",     "lat": 52.35600, "lon": 4.90900, "address": "Weesperzijde 120, 1091 EN Amsterdam"},
    {"kitchen_id": "ams-dapper",     "name": "Casper's Dapperbuurt",     "neighborhood": "Dapperbuurt",      "lat": 52.36250, "lon": 4.92500, "address": "Dapperstraat 40, 1093 BS Amsterdam"},
    {"kitchen_id": "ams-transvaal",  "name": "Casper's Transvaalbuurt",  "neighborhood": "Transvaalbuurt",   "lat": 52.35600, "lon": 4.92300, "address": "Transvaalstraat 20, 1092 HX Amsterdam"},
    {"kitchen_id": "ams-indische",   "name": "Casper's Indische Buurt",  "neighborhood": "Indische Buurt",   "lat": 52.36200, "lon": 4.93600, "address": "Javastraat 80, 1094 HL Amsterdam"},
    {"kitchen_id": "ams-plantage",   "name": "Casper's Plantage",        "neighborhood": "Plantage",         "lat": 52.36600, "lon": 4.91100, "address": "Plantage Middenlaan 40, 1018 DG Amsterdam"},
    {"kitchen_id": "ams-grachten",   "name": "Casper's Grachtengordel",  "neighborhood": "Grachtengordel",   "lat": 52.36600, "lon": 4.89300, "address": "Leidsegracht 10, 1016 CK Amsterdam"},
    {"kitchen_id": "ams-schelde",    "name": "Casper's Scheldebuurt",    "neighborhood": "Scheldebuurt",     "lat": 52.34350, "lon": 4.89900, "address": "Scheldestraat 60, 1078 GJ Amsterdam"},
    {"kitchen_id": "ams-overamstel", "name": "Casper's Overamstel",      "neighborhood": "Overamstel",       "lat": 52.34100, "lon": 4.91300, "address": "Joan Muyskenweg 22, 1096 CJ Amsterdam"},
    {"kitchen_id": "ams-zeeburg",    "name": "Casper's Zeeburg",         "neighborhood": "Zeeburg",          "lat": 52.36900, "lon": 4.94300, "address": "Zeeburgerdijk 200, 1095 AH Amsterdam"},
    {"kitchen_id": "ams-frankendael","name": "Casper's Frankendael",     "neighborhood": "Frankendael",      "lat": 52.34900, "lon": 4.93300, "address": "Middenweg 120, 1098 AR Amsterdam"},
    {"kitchen_id": "ams-diemenwest", "name": "Casper's Diemen-West",     "neighborhood": "Diemen-West",      "lat": 52.34300, "lon": 4.95000, "address": "Ouddiemerlaan 10, 1111 HC Diemen"},
]


def amsterdam_kitchen_rows() -> List[dict]:
    """Rows for the catastrophe_kitchens table, linked to Amsterdam (location_id 7)."""
    rows: List[dict] = []
    for k in AMSTERDAM_KITCHENS:
        rows.append({
            "kitchen_id": k["kitchen_id"],
            "name": k["name"],
            "neighborhood": k["neighborhood"],
            "city": CITIES["amsterdam"].name,
            "location_id": AMSTERDAM_LOCATION_ID,
            "lat": float(k["lat"]),
            "lon": float(k["lon"]),
            "address": k["address"],
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# DevConnect tour cities. Each city has a real central river crossing that acts
# as the choke point when it "closes", plus a nearby parallel crossing used as
# the reroute. The two banks, customer regions and bank "approaches" are all
# DERIVED from these two bridge coordinates (perpendicular to the line between
# them ≈ across the river), so the only per-city curation is:
#   - the two bridge coordinates + names
#   - the river label
#   - a close radius (metres) sized to the river width
#
# ⚠️ COORDINATES ARE APPROXIMATE (eyeballed to a few hundred metres). They are
# good enough because vehicles route on real streets via OSRM and the choke is a
# generous circle — but sanity-check the city you actually present. Bangalore has
# no central river, so its "crossing" is a real traffic choke (Silk Board), not a
# bridge.
# ─────────────────────────────────────────────────────────────────────────────
@dataclass(frozen=True)
class City:
    id: str
    name: str            # display name incl. country, e.g. "Paris, France"
    flag: str
    river: str
    bridge_name: str
    bridge: Tuple[float, float]      # (lat, lon) — the crossing that closes
    alt_name: str
    alt: Tuple[float, float]         # (lat, lon) — reroute crossing (stays open)
    close_radius_m: int = 160


# Bridge coordinates are from OpenStreetMap (Nominatim), so the choke pin lands
# on the real river crossing. The two crossings per city are roughly parallel so
# the line between them runs along the river and the derived banks sit across it.
CITIES: Dict[str, City] = {
    "amsterdam": City(
        "amsterdam", "Amsterdam, Netherlands", "🇳🇱", "Amstel",
        "Berlagebrug", (52.34734, 4.91270),
        "Nieuwe Amstelbrug", (52.35617, 4.90605), 120),
    "montreal": City(
        "montreal", "Montreal, QC", "🇨🇦", "St. Lawrence",
        "Jacques Cartier Bridge", (45.52144, -73.54056),
        "Victoria Bridge", (45.48699, -73.54471), 220),
    "sao_paulo": City(
        "sao_paulo", "São Paulo, Brazil", "🇧🇷", "Pinheiros",
        "Ponte Estaiada", (-23.61211, -46.69962),
        "Ponte Cidade Jardim", (-23.58684, -46.69217), 200),
    "vienna": City(
        "vienna", "Vienna, Austria", "🇦🇹", "Danube",
        "Reichsbrücke", (48.22802, 16.40921),
        "Floridsdorfer Brücke", (48.24944, 16.38958), 220),
    "warsaw": City(
        "warsaw", "Warsaw, Poland", "🇵🇱", "Vistula",
        "Poniatowski Bridge", (52.23565, 21.03829),
        "Świętokrzyski Bridge", (52.24147, 21.03331), 200),
    "paris": City(
        "paris", "Paris, France", "🇫🇷", "Seine",
        "Pont de la Concorde", (48.86338, 2.31960),
        "Pont Alexandre III", (48.86347, 2.31352), 130),
    "lisbon": City(
        "lisbon", "Lisbon, Portugal", "🇵🇹", "Tagus",
        "Ponte 25 de Abril", (38.68917, -9.17694),
        "Ponte Vasco da Gama", (38.76200, -9.04300), 260),
    "washington_dc": City(
        "washington_dc", "Washington, DC", "🇺🇸", "Potomac",
        "Francis Scott Key Bridge", (38.90456, -77.06885),
        "Theodore Roosevelt Bridge", (38.89229, -77.05984), 200),
    "boston": City(
        "boston", "Boston, MA", "🇺🇸", "Charles",
        "Longfellow Bridge", (42.36144, -71.07361),
        "Harvard Bridge", (42.35455, -71.09130), 170),
    "bangalore": City(
        "bangalore", "Bangalore, India", "🇮🇳", "Outer Ring Road",
        "Silk Board Junction", (12.91582, 77.62404),
        "Agara Junction", (12.92214, 77.64794), 200),
    "seoul": City(
        "seoul", "Seoul, South Korea", "🇰🇷", "Han",
        "Banpo Bridge", (37.51456, 126.99651),
        "Hannam Bridge", (37.52671, 127.01338), 260),
    "tokyo": City(
        "tokyo", "Tokyo, Japan", "🇯🇵", "Sumida",
        "Kachidoki Bridge", (35.66226, 139.77490),
        "Eitai Bridge", (35.67664, 139.78615), 200),
    "chicago": City(
        "chicago", "Chicago, IL", "🇺🇸", "Chicago River",
        "DuSable Bridge", (41.88882, -87.62439),
        "Wells Street Bridge", (41.88755, -87.63399), 140),
    "minneapolis": City(
        "minneapolis", "Minneapolis, MN", "🇺🇸", "Mississippi",
        "I-35W St. Anthony Falls Bridge", (44.97948, -93.24479),
        "Hennepin Avenue Bridge", (44.98533, -93.26386), 200),
}


# Hardcoded, city-specific catastrophe per tour stop. Each ties to the city's
# real choke point (the primary bridge/junction that closes). icon+title+desc
# drive the in-app push banner; label is the short status on the choke marker.
# MIRRORED in apps/catastrophe-command/app/main.py (_CITY_CATASTROPHES).
CITY_CATASTROPHES: Dict[str, dict] = {
    "amsterdam":     {"icon": "🌉", "title": "Berlagebrug structural failure",                "desc": "The Berlagebrug's bascule mechanism seized and a span support cracked. The Amstel crossing is shut for emergency structural checks.", "label": "closed"},
    "montreal":      {"icon": "🧊", "title": "Ice storm shuts the Jacques Cartier Bridge",    "desc": "Freezing rain has glazed the deck and ice is falling from the superstructure. The St. Lawrence crossing is fully closed.", "label": "iced over"},
    "sao_paulo":     {"icon": "🌊", "title": "Flash flood on Marginal Pinheiros",             "desc": "A torrential downpour has flooded the Pinheiros riverside; the Ponte Estaiada approaches are underwater.", "label": "flooded"},
    "vienna":        {"icon": "🌉", "title": "Reichsbrücke pier collapse",                    "desc": "A pier has given way and a span has dropped into the Danube. The Reichsbrücke is gone.", "label": "collapsed"},
    "warsaw":        {"icon": "💣", "title": "WWII bomb found by the Poniatowski Bridge",     "desc": "Construction crews uncovered unexploded WWII ordnance. A bomb-disposal cordon has closed the Vistula crossing.", "label": "cordoned off"},
    "paris":         {"icon": "📢", "title": "Protest blocks the Pont de la Concorde",        "desc": "A mass manifestation has flooded Place de la Concorde and blocked the Seine crossing.", "label": "blocked"},
    "lisbon":        {"icon": "🌬️", "title": "Atlantic windstorm closes Ponte 25 de Abril",   "desc": "Extreme crosswinds have forced a full safety closure of Ponte 25 de Abril. Tagus traffic is diverted to Ponte Vasco da Gama.", "label": "closed by wind"},
    "washington_dc": {"icon": "🚓", "title": "Security lockdown on the Key Bridge",           "desc": "A presidential motorcade and Secret Service closure have sealed the Francis Scott Key Bridge over the Potomac.", "label": "locked down"},
    "boston":        {"icon": "🚇", "title": "Red Line derailment on the Longfellow",         "desc": "An MBTA train has derailed on the Longfellow Bridge, which carries the Red Line over the Charles. The bridge is closed.", "label": "derailed"},
    "bangalore":     {"icon": "🚗", "title": "Silk Board gridlock meltdown",                  "desc": "Monsoon waterlogging has turned the Silk Board Junction into total gridlock across the Outer Ring Road.", "label": "gridlocked"},
    "seoul":         {"icon": "🚗", "title": "Major accident on the Banpo Bridge",            "desc": "A multi-vehicle pile-up has blocked all lanes across the Han River on the Banpo Bridge.", "label": "blocked"},
    "tokyo":         {"icon": "📡", "title": "Seismic sensor malfunction shuts the Kachidoki Bridge", "desc": "A faulty seismic sensor triggered a false earthquake alert; the Kachidoki Bridge over the Sumida was automatically shut and awaits inspection.", "label": "closed"},
    "chicago":       {"icon": "🌉", "title": "DuSable Bridge stuck open",                     "desc": "A bascule-lift malfunction during a boat run has left the DuSable Bridge jammed upright over the Chicago River.", "label": "stuck open"},
    "minneapolis":   {"icon": "❄️", "title": "Blizzard pile-up on I-35W",                     "desc": "Whiteout conditions have caused a chain-reaction crash on the I-35W St. Anthony Falls Bridge over the Mississippi.", "label": "blocked"},
}


# Generic neighbourhood labels reused across cities for generated kitchens.
_AREAS: List[str] = [
    "Downtown", "Riverside", "Old Town", "Harbour", "Uptown", "Midtown",
    "North End", "South Side", "East Bank", "West Bank", "Market", "Station",
    "Garden District", "Parkside", "Hillside", "Bayview", "Central", "Latin Quarter",
    "Heights", "Wharf", "The Commons", "Terrace", "Crossing", "Grand Plaza",
]


def _meters_per_deg(lat: float) -> Tuple[float, float]:
    return 111320.0, 111320.0 * math.cos(math.radians(lat))


def _offset(point: Tuple[float, float], east_m: float, north_m: float) -> Tuple[float, float]:
    mlat, mlon = _meters_per_deg(point[0])
    return (point[0] + north_m / mlat, point[1] + east_m / mlon)


def _across_river_unit(bridge: Tuple[float, float], alt: Tuple[float, float]) -> Tuple[float, float]:
    """Unit vector (east, north) roughly perpendicular to the line between the two
    bridges — i.e. pointing across the river."""
    mlat, mlon = _meters_per_deg(bridge[0])
    dx = (alt[1] - bridge[1]) * mlon   # east metres (bridge -> alt, along river)
    dy = (alt[0] - bridge[0]) * mlat   # north metres
    length = math.hypot(dx, dy)
    if length < 1.0:                    # degenerate: default across = east/west
        return 1.0, 0.0
    # perpendicular to (dx, dy)
    px, py = -dy / length, dx / length
    return px, py


def city_config(city_id: str) -> dict:
    """Full geography config the app/client needs for a city. Banks, approaches
    and customer regions are derived from the two bridge coordinates."""
    city = CITIES.get(city_id, CITIES["amsterdam"])
    bridge, alt = city.bridge, city.alt
    center = ((bridge[0] + alt[0]) / 2.0, (bridge[1] + alt[1]) / 2.0)
    px, py = _across_river_unit(bridge, alt)
    region_a = _offset(bridge, px * 1300, py * 1300)
    region_b = _offset(bridge, -px * 1300, -py * 1300)
    approach_a = _offset(bridge, px * 220, py * 220)
    approach_b = _offset(bridge, -px * 220, -py * 220)
    return {
        "id": city.id,
        "name": city.name,
        "flag": city.flag,
        "river": city.river,
        "center": [center[0], center[1]],
        "bridge": {"name": city.bridge_name, "coord": [bridge[0], bridge[1]]},
        "alt": {"name": city.alt_name, "coord": [alt[0], alt[1]]},
        "regions": {"a": [region_a[0], region_a[1]], "b": [region_b[0], region_b[1]]},
        "approaches": {"a": [approach_a[0], approach_a[1]], "b": [approach_b[0], approach_b[1]]},
        "close_radius_m": city.close_radius_m,
        "catastrophe": CITY_CATASTROPHES.get(city.id, CITY_CATASTROPHES["amsterdam"]),
    }


def generate_city_kitchens(city_id: str, *, count: int = 24, seed: int = 2026) -> List[dict]:
    """Deterministic spread of kitchens on both banks around a city's centre.
    Coordinates are approximate; the client snaps them to real roads on load."""
    city = CITIES[city_id]
    cfg = city_config(city_id)
    center = cfg["center"]
    rng = random.Random(f"{city_id}-{seed}")
    radius_m = 2200.0
    rows: List[dict] = []
    for i in range(count):
        angle = rng.uniform(0, 2 * math.pi)
        dist = radius_m * math.sqrt(rng.random())
        lat, lon = _offset((center[0], center[1]), dist * math.cos(angle), dist * math.sin(angle))
        area = _AREAS[i % len(_AREAS)]
        rows.append({
            "kitchen_id": f"{city_id}-{i + 1:02d}",
            "name": f"Casper's {area}",
            "neighborhood": area,
            "city": city.name,
            "location_id": 0,
            "lat": round(lat, 6),
            "lon": round(lon, 6),
            "address": "",
        })
    return rows


def all_city_kitchen_rows(*, seed: int = 2026) -> List[dict]:
    """Kitchen rows for every tour city. Amsterdam uses its hand-curated real
    locations; the rest are generated around each city's centre."""
    rows: List[dict] = list(amsterdam_kitchen_rows())
    for city_id in CITIES:
        if city_id == "amsterdam":
            continue
        rows.extend(generate_city_kitchens(city_id, seed=seed))
    return rows


def scenario_catalog_rows() -> List[dict]:
    rows: List[dict] = []
    for scenario in SCENARIOS.values():
        rows.append(
            {
                "scenario_id": scenario.id,
                "title": scenario.title,
                "description": scenario.description,
                "severity": scenario.severity,
                "eta_minutes": scenario.eta_minutes,
                "blast_radius_km": scenario.blast_radius_km,
                "refund_pressure": scenario.refund_pressure,
                "delivery_delay_multiplier": scenario.delivery_delay_multiplier,
                "inventory_shock": scenario.inventory_shock,
            }
        )
    return rows


def generate_incidents(
    scenario_id: str,
    seed: int,
    location_ids: List[int],
    *,
    incident_count: int = 16,
) -> List[dict]:
    if scenario_id not in SCENARIOS:
        raise ValueError(f"Unknown scenario_id '{scenario_id}'")
    if not location_ids:
        raise ValueError("location_ids cannot be empty")

    scenario = SCENARIOS[scenario_id]
    rng = random.Random(seed)
    base_ts = datetime.now(timezone.utc).replace(microsecond=0)
    incidents: List[dict] = []

    for idx in range(incident_count):
        incidents.append(
            {
                "incident_id": f"{scenario.id}-{seed}-{idx + 1:03d}",
                "scenario_id": scenario.id,
                "city_id": "amsterdam",
                "location_id": int(rng.choice(location_ids)),
                "created_at_utc": base_ts + timedelta(minutes=idx * 3),
                "event_type": rng.choice(
                    [
                        "route_blocked",
                        "driver_rerouted",
                        "order_at_risk",
                        "refund_spike",
                        "inventory_outage",
                    ]
                ),
                "severity": scenario.severity,
                "impact_score": round(min(0.99, 0.35 + rng.random() * scenario.refund_pressure), 3),
                "expected_delay_min": int(10 + rng.random() * 45 * scenario.delivery_delay_multiplier),
                "affected_orders": int(5 + rng.random() * 40),
                "requires_operator_action": bool(rng.random() > 0.35),
            }
        )
    return incidents


def generate_incidents_all_cities(
    scenario_id: str,
    seed: int,
    *,
    incidents_per_city: int = 2,
) -> List[dict]:
    """One incident stream per DevConnect tour city (see CITIES)."""
    if scenario_id not in SCENARIOS:
        raise ValueError(f"Unknown scenario_id '{scenario_id}'")

    scenario = SCENARIOS[scenario_id]
    base_ts = datetime.now(timezone.utc).replace(microsecond=0)
    incidents: List[dict] = []
    offset_min = 0

    for city_id in CITIES:
        rng = random.Random(f"{seed}-{city_id}")
        for idx in range(incidents_per_city):
            incidents.append(
                {
                    "incident_id": f"{scenario.id}-{city_id}-{seed}-{idx + 1:02d}",
                    "scenario_id": scenario.id,
                    "city_id": city_id,
                    "location_id": AMSTERDAM_LOCATION_ID if city_id == "amsterdam" else 0,
                    "created_at_utc": base_ts + timedelta(minutes=offset_min),
                    "event_type": rng.choice(
                        [
                            "route_blocked",
                            "driver_rerouted",
                            "order_at_risk",
                            "refund_spike",
                            "inventory_outage",
                        ]
                    ),
                    "severity": scenario.severity,
                    "impact_score": round(
                        min(0.99, 0.35 + rng.random() * scenario.refund_pressure), 3
                    ),
                    "expected_delay_min": int(
                        10 + rng.random() * 45 * scenario.delivery_delay_multiplier
                    ),
                    "affected_orders": int(5 + rng.random() * 40),
                    "requires_operator_action": bool(rng.random() > 0.35),
                }
            )
            offset_min += 2

    return incidents

