# app/main.py
from pathlib import Path
from typing import Any, Dict, List, Optional
import os
import json
import logging
import uuid
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from .databricks_sql import execute_query

log = logging.getLogger("quest_controller")
logging.basicConfig(level=logging.INFO)

CATALOG = os.getenv("DATABRICKS_CATALOG", "caspersdev")

app = FastAPI(title="Casper's Kitchen Rescue", version="1.0.0")


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


# ─── Static SPA ──────────────────────────────────────────────────────────────

@app.get("/")
def index():
    path = Path(__file__).parent.parent / "index.html"
    if not path.exists():
        raise HTTPException(status_code=404, detail="index.html not found")
    return FileResponse(str(path))


# ─── Game API ────────────────────────────────────────────────────────────────

@app.post("/api/start")
def start_game(body: StartGameRequest):
    player_id = str(uuid.uuid4())[:8]
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")

    levels = execute_query(
        f"SELECT level FROM {CATALOG}.game.quest_levels ORDER BY level"
    )

    if not levels:
        raise HTTPException(status_code=503, detail="Game not initialized yet. Run the Game_Setup stage first.")

    for i, row in enumerate(levels):
        lvl = row["level"]
        status = "active" if i == 0 else "locked"
        execute_query(
            f"""INSERT INTO {CATALOG}.game.quest_state
                (player_id, level, status, started_at, hints_used, score)
                VALUES ('{player_id}', {lvl}, '{status}', '{now}', 0, 0)"""
        )

    return {"player_id": player_id, "player_name": body.player_name}


@app.get("/api/levels")
def get_levels(player_id: Optional[str] = None):
    levels = execute_query(
        f"SELECT * FROM {CATALOG}.game.quest_levels ORDER BY level"
    )

    state_map: Dict[int, Dict] = {}
    if player_id:
        state = execute_query(
            f"""SELECT level, status, score, hints_used, answer, correct
                FROM {CATALOG}.game.quest_state
                WHERE player_id = '{player_id}'"""
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


@app.post("/api/submit")
def submit_answer(body: SubmitAnswerRequest):
    answers_rows = execute_query(
        f"""SELECT acceptable_answers, max_score
            FROM {CATALOG}.game.quest_answers
            WHERE level = {body.level} AND question_key = '{body.question_key}'"""
    )

    if not answers_rows:
        raise HTTPException(status_code=404, detail="Question not found")

    row = answers_rows[0]
    acceptable = row["acceptable_answers"]
    if isinstance(acceptable, str):
        acceptable = json.loads(acceptable)

    max_score = int(row["max_score"])
    player_answer = body.answer.strip().lower()
    correct = any(player_answer == a.lower() for a in acceptable)

    # Fuzzy: also accept substring match for location names
    if not correct:
        correct = any(a.lower() in player_answer for a in acceptable)

    score = max_score if correct else 0

    execute_query(
        f"""UPDATE {CATALOG}.game.quest_state
            SET answer = '{body.answer}',
                correct = {str(correct).lower()},
                score = score + {score}
            WHERE player_id = '{body.player_id}' AND level = {body.level}"""
    )

    # Check if all questions for this level are answered correctly
    all_questions = execute_query(
        f"""SELECT question_key FROM {CATALOG}.game.quest_answers
            WHERE level = {body.level}"""
    )

    response = {
        "correct": correct,
        "score": score,
        "message": "Correct! Great detective work!" if correct else "Not quite. Try again or use a hint.",
    }

    if correct:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        execute_query(
            f"""UPDATE {CATALOG}.game.quest_state
                SET status = 'completed', completed_at = '{now}'
                WHERE player_id = '{body.player_id}' AND level = {body.level}"""
        )

        # Unlock next level
        next_level = body.level + 1
        next_exists = execute_query(
            f"SELECT level FROM {CATALOG}.game.quest_levels WHERE level = {next_level}"
        )
        if next_exists:
            execute_query(
                f"""UPDATE {CATALOG}.game.quest_state
                    SET status = 'active'
                    WHERE player_id = '{body.player_id}' AND level = {next_level}"""
            )
            response["next_level_unlocked"] = next_level

    return response


@app.post("/api/hint")
def get_hint(body: HintRequest):
    hint_rows = execute_query(
        f"""SELECT hint FROM {CATALOG}.game.quest_answers
            WHERE level = {body.level} AND question_key = '{body.question_key}'"""
    )

    if not hint_rows:
        raise HTTPException(status_code=404, detail="No hint available")

    execute_query(
        f"""UPDATE {CATALOG}.game.quest_state
            SET hints_used = hints_used + 1, score = GREATEST(score - 10, 0)
            WHERE player_id = '{body.player_id}' AND level = {body.level}"""
    )

    return {"hint": hint_rows[0]["hint"]}


@app.get("/api/leaderboard")
def get_leaderboard():
    rows = execute_query(
        f"""SELECT player_id,
                   SUM(score) as total_score,
                   COUNT(CASE WHEN status = 'completed' THEN 1 END) as levels_completed,
                   SUM(hints_used) as total_hints
            FROM {CATALOG}.game.quest_state
            GROUP BY player_id
            ORDER BY total_score DESC
            LIMIT 20"""
    )
    return {"leaderboard": rows}


@app.get("/api/player/{player_id}/score")
def get_player_score(player_id: str):
    rows = execute_query(
        f"""SELECT SUM(score) as total_score,
                   COUNT(CASE WHEN status = 'completed' THEN 1 END) as levels_completed,
                   SUM(hints_used) as total_hints
            FROM {CATALOG}.game.quest_state
            WHERE player_id = '{player_id}'"""
    )
    if not rows:
        raise HTTPException(status_code=404, detail="Player not found")
    return rows[0]


# ─── Config (tool URLs) ──────────────────────────────────────────────────────

@app.get("/api/config")
def get_config():
    try:
        rows = execute_query(
            f"SELECT config_key, config_value FROM {CATALOG}.game.config"
        )
        return {r["config_key"]: r["config_value"] for r in rows}
    except Exception:
        return {}


# ─── Health & Debug ──────────────────────────────────────────────────────────

@app.get("/healthz")
def healthz():
    return {"ok": True}


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
