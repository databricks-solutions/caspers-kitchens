import os
import sys

import agent  # noqa: F401 - registers @invoke with MLflow AgentServer
from mlflow.genai.agent_server import AgentServer


agent_server = AgentServer("ResponsesAgent")
app = agent_server.app


def main():
    app_port = os.environ.get("DATABRICKS_APP_PORT")
    if app_port and "--port" not in sys.argv:
        sys.argv.extend(["--port", app_port])
    agent_server.run(app_import_string="start_server:app")


if __name__ == "__main__":
    main()
