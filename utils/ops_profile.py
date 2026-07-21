"""Operational dashboard agent profile — which Genie spaces and KAs to deploy.

Job parameters ``GENIE_SPACE_KEYS`` and ``KA_KEYS`` are comma-separated lists.
Other targets omit them (stages default to the full set).  The ``free`` target
sets ``GENIE_SPACE_KEYS=revenue`` and ``KA_KEYS=inspection`` for a minimal demo.
"""

from __future__ import annotations

GENIE_KEYS = ("revenue", "ops", "menu")
KA_KEYS = ("inspection", "menu", "legal", "regulatory", "audits", "consultancy")

GENIE_TITLE_BASE = {
    "revenue": "Revenue & Orders Intelligence",
    "ops": "Operations Intelligence",
    "menu": "Menu & Safety Intelligence",
}

KA_DISPLAY_SUFFIX = {
    "inspection": "inspection-knowledge",
    "menu": "menu-knowledge",
    "legal": "legal",
    "regulatory": "regulatory",
    "audits": "audits",
    "consultancy": "consultancy",
}

KA_VOLUME_REL_PATHS = {
    "inspection": ("Inspection Reports", "food_safety/reports"),
    "menu": ("Menu Documents", "menu_documents/menus"),
    "legal": ("Legal Complaints", "legal_complaints/documents"),
    "regulatory": ("Regulatory Docs", "regulatory/documents"),
    "audits": ("Audit Reports", "audits/reports"),
    "consultancy": ("Consultancy Reports", "consultancy/reports"),
}

GENIE_DOMAIN_TAGS = {
    "revenue": ["revenue"],
    "ops": ["operations"],
    "menu": ["compliance"],
}

DEFAULT_GENIE_SPACE_KEYS = "revenue,ops,menu"
DEFAULT_KA_KEYS = "inspection,menu,legal,regulatory,audits,consultancy"


def parse_csv_keys(raw: str, allowed: tuple[str, ...], default: str, label: str) -> list[str]:
    value = (raw or default).strip()
    keys = [k.strip() for k in value.split(",") if k.strip()]
    if not keys:
        keys = [k.strip() for k in default.split(",") if k.strip()]
    unknown = [k for k in keys if k not in allowed]
    if unknown:
        raise ValueError(f"Unknown {label} value(s) {unknown}; allowed: {list(allowed)}")
    return keys


def genie_title(catalog: str, key: str) -> str:
    return f"{GENIE_TITLE_BASE[key]} ({catalog})"


def ka_display_name(catalog: str, key: str) -> str:
    return f"{catalog}-{KA_DISPLAY_SUFFIX[key]}"


def ka_volume_paths(catalog: str, keys: list[str]) -> list[tuple[str, str]]:
    return [
        (label, f"/Volumes/{catalog}/{rel}")
        for key in keys
        for label, rel in [KA_VOLUME_REL_PATHS[key]]
    ]


def build_supervisor_agents(
  *,
  genie_ids: dict[str, str],
  ka_endpoints: dict[str, str],
) -> list[dict]:
    """Build MAS sub-agent list for the selected Genie spaces and KAs only."""
    agents: list[dict] = []

    genie_specs = {
        "revenue": {
            "agent_type": "genie",
            "name": "revenue-analytics",
            "description": (
                "ONLY call for questions about: revenue ($), sales totals, order counts, average order "
                "value, brand sales rankings, location revenue comparisons, or financial performance "
                "metrics. Keywords: revenue, sales, earned, orders, top performing, best/worst location "
                "by revenue, avg order, financial. Do NOT call for food safety, inspections, legal, "
                "audits, menus, or strategic questions."
            ),
        },
        "ops": {
            "agent_type": "genie",
            "name": "operations-intelligence",
            "description": (
                "ONLY call for questions about: order throughput, kitchen operations, delivery performance, "
                "food safety inspection scores, food safety violation counts, location health metrics, "
                "or operational risk. Keywords: operations, kitchen, delivery, inspection score, food "
                "safety grade, throughput, busiest hours, peak demand, operational issues, cancel rate, "
                "complaint rate. Do NOT call for revenue totals, legal cases, audit reports, or strategy. "
                "IMPORTANT: 'complaints' here means customer operational complaints — NOT legal filings."
            ),
        },
        "menu": {
            "agent_type": "genie",
            "name": "menu-analytics",
            "description": (
                "ONLY call for structured queries about menu items, nutrition, allergens, item pricing "
                "tiers, brand-level menu comparisons, or per-location compliance summaries. Keywords: "
                "calories, protein, fat, carbs, allergen-free, gluten-free, dairy-free, item price, "
                "brand menu, nutrition comparison, healthy options. Do NOT call for descriptive "
                "questions about individual dishes — use menu-document-search for those."
            ),
        },
    }

    ka_specs = {
        "inspection": {
            "agent_type": "ka",
            "name": "inspection-reports",
            "description": (
                "ONLY call for questions about specific food safety inspection documents, detailed "
                "inspector findings, corrective action details, or historical inspection report text. "
                "For inspection SCORES/GRADES use operations-intelligence instead."
            ),
        },
        "menu": {
            "agent_type": "ka",
            "name": "menu-document-search",
            "description": (
                "ONLY call for descriptive questions about specific dishes — ingredient details, "
                "preparation, dish description, or specific menu content. For nutrition stats, "
                "allergen filtering, or price comparisons, use menu-analytics instead."
            ),
        },
        "legal": {
            "agent_type": "ka",
            "name": "legal-complaints",
            "description": (
                "ONLY call for questions about lawsuits, legal cases, customer complaint filings, "
                "litigation status, legal liability, legal issues, or specific complaint case numbers. "
                "Always surface case numbers (format CK-XX-XXXX), risk levels (HIGH/MEDIUM/LOW), "
                "and amounts at stake."
            ),
        },
        "regulatory": {
            "agent_type": "ka",
            "name": "regulatory-compliance",
            "description": (
                "ONLY call for questions about permits, licenses, certifications, regulatory compliance "
                "documents, or specific regulatory requirements."
            ),
        },
        "audits": {
            "agent_type": "ka",
            "name": "audit-findings",
            "description": (
                "ONLY call for questions about financial or operational audit reports, audit findings, "
                "auditor recommendations, or specific audit references."
            ),
        },
        "consultancy": {
            "agent_type": "ka",
            "name": "consultancy-strategy",
            "description": (
                "ONLY call for questions about strategic recommendations, consultant advice, improvement "
                "strategies, or specific consultancy reports."
            ),
        },
    }

    for key, space_id in genie_ids.items():
        spec = genie_specs[key].copy()
        spec["genie_space"] = {"id": space_id}
        agents.append(spec)

    for key, endpoint_name in ka_endpoints.items():
        spec = ka_specs[key].copy()
        spec["serving_endpoint"] = {"name": endpoint_name}
        agents.append(spec)

    return agents
