from pathlib import Path
from mcp.server.fastmcp import FastMCP
from fastapi import FastAPI
from fastapi.responses import FileResponse
from .demo_controller import DemoController

STATIC_DIR = Path(__file__).parent / "static"

# Create an MCP server
mcp = FastMCP("Casper's Demo Controller")
demo_controller = DemoController()


@mcp.tool()
def start_demo(catalog: str) -> dict:
    """Start Casper's demo with specified catalog name"""
    return demo_controller.start_demo(catalog)


@mcp.tool()
def get_demo_status(catalog: str = None) -> dict:
    """Get status of demo. Optionally specify catalog to check specific demo"""
    return demo_controller.get_demo_status(catalog)


@mcp.tool()
def cleanup_demo(catalog: str) -> dict:
    """Clean up demo resources for specified catalog"""
    return demo_controller.cleanup_demo(catalog)


# Add an addition tool
@mcp.tool()
def add(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b


# Add a dynamic greeting resource
@mcp.resource("greeting://{name}")
def get_greeting(name: str) -> str:
    """Get a personalized greeting"""
    return f"Hello, {name}!"


mcp_app = mcp.streamable_http_app()


app = FastAPI(
    lifespan=lambda _: mcp.session_manager.run(),
)


@app.get("/", include_in_schema=False)
async def serve_index():
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/", mcp_app)
