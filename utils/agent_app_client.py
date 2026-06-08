"""Helpers for DAIS custom agents deployed as Databricks Apps."""

from __future__ import annotations

import hashlib
import json
import os
import re
from typing import Any, Iterable

import requests
from databricks.sdk import WorkspaceClient


_APP_NAME_MAX_LEN = 30
_APP_NAME_SAFE = re.compile(r"[^a-z0-9-]+")


def _safe_app_name(value: str) -> str:
    normalized = _APP_NAME_SAFE.sub("-", value.lower()).strip("-")
    normalized = re.sub(r"-+", "-", normalized)
    if not normalized:
        raise ValueError("App name cannot be empty")
    if len(normalized) <= _APP_NAME_MAX_LEN:
        return normalized

    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:6]
    prefix_len = _APP_NAME_MAX_LEN - len(digest) - 1
    prefix = normalized[:prefix_len].rstrip("-")
    return f"{prefix}-{digest}"


def refund_agent_app_name(catalog: str) -> str:
    return _safe_app_name(f"refund-agent-{catalog}")


def complaint_agent_app_name(catalog: str) -> str:
    return _safe_app_name(f"complaint-agent-{catalog}")


def get_notebook_token(dbutils: Any) -> str:
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    token = ctx.apiToken().get()
    if not token:
        raise RuntimeError("Notebook API token is unavailable")
    return token


def exchange_notebook_token_for_app_token(
    *,
    host: str,
    notebook_token: str,
    app_oauth_client_id: str,
    timeout: float = 30,
) -> str:
    response = requests.post(
        url=f"{host.rstrip('/')}/oidc/v1/token",
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:token-exchange",
            "subject_token": notebook_token,
            "subject_token_type": "urn:databricks:params:oauth:token-type:personal-access-token",
            "requested_token_type": "urn:ietf:params:oauth:token-type:access_token",
            "scope": "all-apis",
            "audience": app_oauth_client_id,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    token = response.json().get("access_token")
    if not token:
        raise RuntimeError("Databricks token exchange returned no access_token")
    return token


def app_bearer_token(
    *,
    app_name: str,
    w: WorkspaceClient | None = None,
    dbutils: Any | None = None,
    timeout: float = 30,
) -> str:
    w = w or WorkspaceClient()
    if dbutils is None:
        header = w.config.authenticate().get("Authorization", "")
        if not header.startswith("Bearer "):
            raise RuntimeError("Databricks OAuth bearer token is unavailable")
        return header.removeprefix("Bearer ")

    app = w.apps.get(app_name)
    client_id = getattr(app, "oauth2_app_client_id", None)
    if not client_id:
        raise RuntimeError(f"App {app_name!r} has no oauth2_app_client_id")
    return exchange_notebook_token_for_app_token(
        host=w.config.host,
        notebook_token=get_notebook_token(dbutils),
        app_oauth_client_id=client_id,
        timeout=timeout,
    )


def app_url(app_name: str, w: WorkspaceClient | None = None) -> str:
    app = (w or WorkspaceClient()).apps.get(app_name)
    url = getattr(app, "url", None)
    if not url:
        raise RuntimeError(f"App {app_name!r} has no URL")
    return url.rstrip("/")


def app_request_context(
    *,
    app_name: str,
    w: WorkspaceClient | None = None,
    dbutils: Any | None = None,
    timeout: float = 30,
) -> dict[str, str]:
    w = w or WorkspaceClient()
    return {
        "app_name": app_name,
        "url": app_url(app_name, w=w),
        "bearer_token": app_bearer_token(
            app_name=app_name,
            w=w,
            dbutils=dbutils,
            timeout=timeout,
        ),
    }


def call_agent_app(
    *,
    app_name: str,
    input_messages: list[dict[str, Any]],
    w: WorkspaceClient | None = None,
    dbutils: Any | None = None,
    timeout: float = 120,
    extra_body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    w = w or WorkspaceClient()
    body: dict[str, Any] = {"input": input_messages}
    if extra_body:
        body.update(extra_body)

    token = app_bearer_token(app_name=app_name, w=w, dbutils=dbutils, timeout=min(timeout, 30))
    response = requests.post(
        url=f"{app_url(app_name, w=w)}/responses",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json=body,
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def extract_response_text(response: Any) -> str:
    if isinstance(response, str):
        try:
            response = json.loads(response)
        except json.JSONDecodeError:
            return response

    if hasattr(response, "model_dump"):
        response = response.model_dump(mode="json")
    elif hasattr(response, "dict"):
        response = response.dict()

    if not isinstance(response, dict):
        raise TypeError(f"Unsupported response type: {type(response).__name__}")

    direct = response.get("output_text")
    if isinstance(direct, str) and direct:
        return direct

    for item in _iter_response_items(response.get("output", [])):
        content = item.get("content")
        if isinstance(content, str) and content:
            return content
        for content_item in _iter_response_items(content or []):
            text = content_item.get("text")
            if isinstance(text, str) and text:
                return text

    raise ValueError(f"Could not extract output text from response keys: {sorted(response.keys())}")


def call_agent_app_text(**kwargs: Any) -> str:
    return extract_response_text(call_agent_app(**kwargs))


def gateway_chat_probe(
    *,
    llm_model: str,
    w: WorkspaceClient | None = None,
    dbutils: Any | None = None,
    timeout: float = 30,
) -> None:
    w = w or WorkspaceClient()
    if dbutils is not None:
        token = get_notebook_token(dbutils)
    else:
        header = w.config.authenticate().get("Authorization", "")
        if not header.startswith("Bearer "):
            raise RuntimeError("Databricks OAuth bearer token is unavailable")
        token = header.removeprefix("Bearer ")

    response = requests.post(
        url=f"{w.config.host.rstrip('/')}/ai-gateway/mlflow/v1/chat/completions",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={
            "model": llm_model,
            "messages": [{"role": "user", "content": "Say gateway ok."}],
            "max_tokens": 8,
        },
        timeout=timeout,
    )
    response.raise_for_status()


def _iter_response_items(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
    elif isinstance(value, list):
        for item in value:
            if isinstance(item, dict):
                yield item
