"""City choke-point names for route_policies SQL. Keep in sync with
data/canonical/catastrophe_scenarios.py CITIES and main.py _CITIES."""

from __future__ import annotations

# (city_id, bridge_name, alt_name, river)
CROSSINGS: tuple[tuple[str, str, str, str], ...] = (
    ("amsterdam", "Berlagebrug", "Nieuwe Amstelbrug", "Amstel"),
    ("montreal", "Jacques Cartier Bridge", "Victoria Bridge", "St. Lawrence"),
    ("sao_paulo", "Ponte Estaiada", "Ponte Cidade Jardim", "Pinheiros"),
    ("vienna", "Reichsbrücke", "Floridsdorfer Brücke", "Danube"),
    ("warsaw", "Poniatowski Bridge", "Świętokrzyski Bridge", "Vistula"),
    ("paris", "Pont de la Concorde", "Pont Alexandre III", "Seine"),
    ("washington_dc", "Francis Scott Key Bridge", "Theodore Roosevelt Bridge", "Potomac"),
    ("boston", "Longfellow Bridge", "Harvard Bridge", "Charles"),
    ("bangalore", "Silk Board Junction", "Agara Junction", "Outer Ring Road"),
    ("seoul", "Banpo Bridge", "Hannam Bridge", "Han"),
    ("tokyo", "Kachidoki Bridge", "Eitai Bridge", "Sumida"),
    ("chicago", "DuSable Bridge", "Wells Street Bridge", "Chicago River"),
    ("minneapolis", "I-35W St. Anthony Falls Bridge", "Hennepin Avenue Bridge", "Mississippi"),
)
