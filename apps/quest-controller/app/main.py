# app/main.py
from pathlib import Path
from typing import Any, Dict, List, Optional
import os
import json
import logging
import time
import uuid
from datetime import datetime, timezone

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from .databricks_sql import execute_query
from .lakebase import execute_pg

log = logging.getLogger("quest_controller")
logging.basicConfig(level=logging.INFO)

CATALOG = os.getenv("DATABRICKS_CATALOG", "caspersdev")
CACHE_TTL_SECONDS = 300  # reload caches at most every 5 minutes

app = FastAPI(title="Casper's Kitchen Rescue", version="1.0.0")


# ─── In-memory caches ────────────────────────────────────────────────────────

_answers_cache: Dict[str, Dict] = {}   # "(level):(key)" -> {acceptable_answers, max_score, hint}
_levels_cache: List[Dict] = []
_levels_set: set = set()
_config_cache: Dict[str, str] = {}
_cache_loaded_at: float = 0


def _ensure_published_dashboard_url(url: str) -> str:
    """Ensure dashboard URL opens published view, not edit mode."""
    if not url or "dashboardsv3" not in url:
        return url
    try:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(url)
        params = parse_qs(parsed.query, keep_blank_values=True)
        params["view"] = ["published"]
        new_query = urlencode(params, doseq=True)
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))
    except Exception:
        return url


def _config_with_published_dashboard(cfg: Dict[str, str]) -> Dict[str, str]:
    """Return config with dashboard_url forced to published view."""
    out = dict(cfg)
    if "dashboard_url" in out:
        out["dashboard_url"] = _ensure_published_dashboard_url(out["dashboard_url"])
    return out


def _load_caches(force: bool = False):
    """Reload answers, levels and config from the database (with TTL guard)."""
    global _answers_cache, _levels_cache, _levels_set, _config_cache, _cache_loaded_at

    if not force and _answers_cache and (time.monotonic() - _cache_loaded_at) < CACHE_TTL_SECONDS:
        return

    try:
        new_answers: Dict[str, Dict] = {}
        rows = execute_query(
            f"SELECT level, question_key, acceptable_answers, max_score, hint "
            f"FROM {CATALOG}.game.quest_answers"
        )
        for r in rows:
            acceptable = r["acceptable_answers"]
            if isinstance(acceptable, str):
                acceptable = json.loads(acceptable)
            cache_key = f"{r['level']}:{r['question_key']}"
            new_answers[cache_key] = {
                "acceptable_answers": acceptable,
                "max_score": int(r["max_score"]),
                "hint": r["hint"],
            }
        _answers_cache = new_answers
        log.info("Cached %d answer entries", len(_answers_cache))
    except Exception as e:
        log.warning("Failed to cache answers: %s", e)

    try:
        _levels_cache = execute_query(
            f"SELECT * FROM {CATALOG}.game.quest_levels ORDER BY level"
        )
        _levels_set = {int(lv["level"]) for lv in _levels_cache}
        log.info("Cached %d levels", len(_levels_cache))
    except Exception as e:
        log.warning("Failed to cache levels: %s", e)

    try:
        rows = execute_query(
            f"SELECT config_key, config_value FROM {CATALOG}.game.config"
        )
        _config_cache = {r["config_key"]: r["config_value"] for r in rows}
        log.info("Cached %d config entries", len(_config_cache))
    except Exception as e:
        log.warning("Failed to cache config: %s", e)

    _cache_loaded_at = time.monotonic()


@app.on_event("startup")
def on_startup():
    _load_caches(force=True)


@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    log.exception("Unhandled error: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": str(exc)},
    )


# ─── Models ──────────────────────────────────────────────────────────────────

class StartGameRequest(BaseModel):
    player_name: str


class SubmitAnswerRequest(BaseModel):
    player_id: str
    level: int
    question_key: str
    answer: str


class HintRequest(BaseModel):
    player_id: str
    level: int
    question_key: str


class ResetRequest(BaseModel):
    player_id: str


# ─── Static SPA ──────────────────────────────────────────────────────────────

@app.get("/")
def index():
    path = Path(__file__).parent.parent / "index.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(path), headers={"Cache-Control": "no-cache, no-store, must-revalidate"})


@app.get("/favicon.svg")
def favicon():
    path = Path(__file__).parent.parent / "favicon.svg"
    if not path.exists():
        raise HTTPException(status_code=404)
    return FileResponse(str(path), media_type="image/svg+xml")


# ─── Game API ────────────────────────────────────────────────────────────────

def _register_new_player(player_id: str, player_name: str, now: str):
    """Background task: persist leaderboard + quest_state rows for a new player."""
    try:
        execute_pg(
            """INSERT INTO leaderboard
                (player_id, player_name, total_score, levels_completed, total_hints_used, started_at)
                VALUES (%s, %s, 0, 0, 0, %s)
                ON CONFLICT (player_id) DO NOTHING""",
            (player_id, player_name, now),
        )
        for i, lv in enumerate(_levels_cache):
            lvl = lv["level"]
            status = "active" if i == 0 else "locked"
            execute_pg(
                """INSERT INTO quest_state
                    (player_id, level, status, started_at, hints_used, score)
                    VALUES (%s, %s, %s, %s, 0, 0)
                    ON CONFLICT (player_id, level) DO NOTHING""",
                (player_id, lvl, status, now),
            )
        log.info("Registered new player %s (%s)", player_name, player_id)
    except Exception as e:
        log.error("Failed to register player %s: %s", player_name, e)


def _build_levels_response(state_map: Dict[int, Dict]) -> List[Dict]:
    result = []
    for i, lv in enumerate(_levels_cache):
        lvl_num = int(lv["level"])
        ps = state_map.get(lvl_num, {})
        result.append({
            "level": lvl_num,
            "title": lv["title"],
            "subtitle": lv["subtitle"],
            "story": lv["story"],
            "feature": lv["feature"],
            "instructions": lv["instructions"],
            "question_keys": lv.get("question_keys", []),
            "status": ps.get("status", "active" if i == 0 else "locked"),
            "score": int(ps.get("score", 0) or 0),
            "hints_used": int(ps.get("hints_used", 0) or 0),
        })
    return result


@app.post("/api/start")
def start_game(body: StartGameRequest, bg: BackgroundTasks):
    # Reload all caches so answers, levels, and tool URLs are always fresh
    _load_caches()

    if not _levels_cache:
        raise HTTPException(status_code=503, detail="Game not initialized yet. Run the Game_Setup stage first.")

    safe_name = body.player_name.strip()

    existing = execute_pg(
        """SELECT lb.player_id, qs.level, qs.status, qs.score, qs.hints_used
            FROM leaderboard lb
            JOIN quest_state qs ON lb.player_id = qs.player_id
            WHERE lb.player_name = %s""",
        (safe_name,),
    )

    if existing:
        player_id = existing[0]["player_id"]
        state_map: Dict[int, Dict] = {}
        for row in existing:
            state_map[int(row["level"])] = row

        return {
            "player_id": player_id,
            "player_name": body.player_name.strip(),
            "levels": _build_levels_response(state_map),
            "config": _config_with_published_dashboard(_config_cache),
            "resumed": True,
        }

    # New player
    player_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    bg.add_task(_register_new_player, player_id, body.player_name.strip(), now)

    return {
        "player_id": player_id,
        "player_name": body.player_name.strip(),
        "levels": _build_levels_response({}),
        "config": _config_with_published_dashboard(_config_cache),
        "resumed": False,
    }


@app.get("/api/warmup")
def warmup():
    """Lightweight probe to check Lakebase connectivity."""
    try:
        execute_pg("SELECT 1")
        return {"warm": True}
    except Exception:
        return {"warm": False}


@app.get("/api/levels")
def get_levels(player_id: Optional[str] = None):
    levels = execute_query(
        f"SELECT * FROM {CATALOG}.game.quest_levels ORDER BY level"
    )

    state_map: Dict[int, Dict] = {}
    if player_id:
        state = execute_pg(
            """SELECT level, status, score, hints_used, answer, correct
                FROM quest_state
                WHERE player_id = %s""",
            (player_id,),
        )
        for s in state:
            state_map[int(s["level"])] = s

    result = []
    for lv in levels:
        lvl_num = int(lv["level"])
        ps = state_map.get(lvl_num, {})
        result.append({
            "level": lvl_num,
            "title": lv["title"],
            "subtitle": lv["subtitle"],
            "story": lv["story"],
            "feature": lv["feature"],
            "instructions": lv["instructions"],
            "question_keys": lv.get("question_keys", []),
            "status": ps.get("status", "locked"),
            "score": ps.get("score", 0),
            "hints_used": ps.get("hints_used", 0),
        })
    return {"levels": result}


def _persist_submit(player_id: str, level: int, answer: str, correct: bool, score: int, next_level: Optional[int]):
    """Background: persist answer result to DB."""
    try:
        if correct:
            now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
            execute_pg(
                """UPDATE quest_state
                    SET answer = %s, correct = true,
                        score = score + %s, status = 'completed', completed_at = %s
                    WHERE player_id = %s AND level = %s""",
                (answer, score, now, player_id, level),
            )
            if next_level is not None:
                execute_pg(
                    """UPDATE quest_state
                        SET status = 'active'
                        WHERE player_id = %s AND level = %s""",
                    (player_id, next_level),
                )
        else:
            execute_pg(
                """UPDATE quest_state
                    SET answer = %s, correct = false
                    WHERE player_id = %s AND level = %s""",
                (answer, player_id, level),
            )
    except Exception as e:
        log.error("Failed to persist submit: %s", e)


@app.post("/api/submit")
def submit_answer(body: SubmitAnswerRequest, bg: BackgroundTasks):
    cache_key = f"{body.level}:{body.question_key}"
    cached = _answers_cache.get(cache_key)
    if not cached:
        raise HTTPException(status_code=404, detail="Question not found")

    acceptable = cached["acceptable_answers"]
    max_score = cached["max_score"]
    player_answer = body.answer.strip().lower()
    correct = any(player_answer == a.lower() for a in acceptable)

    if not correct:
        correct = any(a.lower() in player_answer for a in acceptable)

    score = max_score if correct else 0
    next_level = body.level + 1 if correct and (body.level + 1) in _levels_set else None

    bg.add_task(_persist_submit, body.player_id, body.level, body.answer, correct, score, next_level)

    if not correct:
        return {"correct": False, "score": 0, "message": "Not quite. Try again or use a hint."}

    response = {"correct": True, "score": score, "message": "Correct! Great detective work!"}
    if next_level is not None:
        response["next_level_unlocked"] = next_level
    return response


def _persist_hint(player_id: str, level: int):
    """Background: persist hint usage to DB."""
    try:
        execute_pg(
            """UPDATE quest_state
                SET hints_used = hints_used + 1, score = GREATEST(score - 10, 0)
                WHERE player_id = %s AND level = %s""",
            (player_id, level),
        )
    except Exception as e:
        log.error("Failed to persist hint: %s", e)


@app.post("/api/reset")
def reset_progress(body: ResetRequest):
    """Delete player's progress so they can start again."""
    try:
        execute_pg("DELETE FROM quest_state WHERE player_id = %s", (body.player_id,))
        execute_pg("DELETE FROM leaderboard WHERE player_id = %s", (body.player_id,))
        return {"ok": True}
    except Exception as e:
        log.exception("Reset failed: %s", e)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/hint")
def get_hint(body: HintRequest, bg: BackgroundTasks):
    cache_key = f"{body.level}:{body.question_key}"
    cached = _answers_cache.get(cache_key)
    if not cached or not cached.get("hint"):
        raise HTTPException(status_code=404, detail="No hint available")

    bg.add_task(_persist_hint, body.player_id, body.level)
    return {"hint": cached["hint"]}


# Fixed fake users for leaderboard comparison (name -> total_score)
_FAKE_LEADERBOARD = [
    {"player_name": "Ali", "total_score": 100000001, "levels_completed": 5, "total_hints": 0},
    {"player_name": "Matei", "total_score": 100000000, "levels_completed": 5, "total_hints": 0},
    {"player_name": "Holly", "total_score": 12345, "levels_completed": 5, "total_hints": 0},
    {"player_name": "Nick", "total_score": 350, "levels_completed": 2, "total_hints": 0},
    {"player_name": "Youssef", "total_score": -300, "levels_completed": 0, "total_hints": 30},
]


@app.get("/api/leaderboard")
def get_leaderboard():
    rows = execute_pg(
        """SELECT lb.player_name,
                   SUM(qs.score) as total_score,
                   COUNT(CASE WHEN qs.status = 'completed' THEN 1 END) as levels_completed,
                   SUM(qs.hints_used) as total_hints
            FROM quest_state qs
            JOIN leaderboard lb ON qs.player_id = lb.player_id
            GROUP BY lb.player_name
            ORDER BY total_score DESC
            LIMIT 20"""
    )
    # Merge with fake users (fixed scores for comparison); real users added unless name collision
    fake_names = {r["player_name"] for r in _FAKE_LEADERBOARD}
    by_name = {r["player_name"]: dict(r) for r in _FAKE_LEADERBOARD}
    for r in rows:
        if r["player_name"] not in fake_names:
            by_name[r["player_name"]] = dict(r)
    merged = sorted(by_name.values(), key=lambda x: (x.get("total_score") or 0), reverse=True)
    return {"leaderboard": merged}


@app.get("/api/player/{player_id}/score")
def get_player_score(player_id: str):
    rows = execute_pg(
        """SELECT SUM(score) as total_score,
                   COUNT(CASE WHEN status = 'completed' THEN 1 END) as levels_completed,
                   SUM(hints_used) as total_hints
            FROM quest_state
            WHERE player_id = %s""",
        (player_id,),
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Player not found")
    return rows[0]


# ─── Config (tool URLs) ──────────────────────────────────────────────────────

@app.get("/api/config")
def get_config():
    """Always read config from DB — tool URLs may be written after app startup."""
    try:
        rows = execute_query(
            f"SELECT config_key, config_value FROM {CATALOG}.game.config"
        )
        fresh = {r["config_key"]: r["config_value"] for r in rows}
        _config_cache.update(fresh)
        return _config_with_published_dashboard(fresh)
    except Exception as e:
        log.warning("Config read failed, returning cache: %s", e)
        return _config_with_published_dashboard(_config_cache)


# ─── Health & Debug ──────────────────────────────────────────────────────────

@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.post("/api/refresh")
def refresh_caches():
    """Force-reload all caches from the database."""
    _load_caches(force=True)
    return {
        "answers": len(_answers_cache),
        "levels": len(_levels_cache),
        "config_keys": list(_config_cache.keys()),
    }


@app.get("/api/debug")
def debug_info():
    from .databricks_sql import WAREHOUSE_ID as wh_id, CATALOG as cat
    info = {
        "catalog": cat,
        "warehouse_id": wh_id or "(NOT SET)",
        "env_DATABRICKS_WAREHOUSE_ID": os.getenv("DATABRICKS_WAREHOUSE_ID", "(NOT SET)"),
        "env_DATABRICKS_CATALOG": os.getenv("DATABRICKS_CATALOG", "(NOT SET)"),
    }
    try:
        from .databricks_sql import _get_client
        w = _get_client()
        info["sdk_host"] = w.config.host
        info["sdk_auth"] = "ok"
    except Exception as e:
        info["sdk_auth"] = f"FAILED: {e}"

    try:
        from .databricks_sql import execute_query as eq
        eq(f"SELECT 1 AS ping")
        info["sql_query"] = "ok"
    except Exception as e:
        info["sql_query"] = f"FAILED: {e}"

    return info
