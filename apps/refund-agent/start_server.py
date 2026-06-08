import inspect
import json
import os

import agent  # noqa: F401 - registers @invoke with MLflow AgentServer
from fastapi import Request
from fastapi.responses import JSONResponse
from mlflow.genai.agent_server import AgentServer, setup_mlflow_git_based_version_tracking
from mlflow.types.responses import ResponsesAgentRequest


agent_server = AgentServer("ResponsesAgent")
app = agent_server.app

setup_mlflow_git_based_version_tracking()


@app.post("/responses")
@app.post("/api/responses")
async def responses(request: Request):
    """Databricks Apps agent-compatible Responses API alias.

    MLflow AgentServer serves /invocations locally. Databricks Apps agent
    clients use /responses, so expose the same registered invoke function on
    that route too.
    """
    from mlflow.genai.agent_server import get_invoke_function

    body = await request.json()
    if body.get("stream"):
        return JSONResponse(
            status_code=400,
            content={"error": "stream=true is not supported by this agent app"},
        )

    invoke_fn = get_invoke_function()
    result = invoke_fn(ResponsesAgentRequest(**body))
    if inspect.isawaitable(result):
        result = await result
    return JSONResponse(content=result.model_dump(mode="json"))


def main():
    port = int(os.environ.get("DATABRICKS_APP_PORT", "8000"))
    agent_server.run(
        app_import_string="start_server:app",
        host="0.0.0.0",
        port=port,
    )


if __name__ == "__main__":
    main()
