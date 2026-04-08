#!/usr/bin/env python3
"""
Casper's Kitchen Rescue — Grant Player Access

Gives a user all permissions needed to play the game:
  - UC grants (catalog, schemas, volume)
  - SQL warehouse access
  - Genie room sharing
  - Dashboard sharing
  - Knowledge Assistant sharing

Usage:
  python scripts/grant_player_access.py player@example.com --catalog caspersdev
  python scripts/grant_player_access.py player@example.com  # defaults to caspersdev
"""

import argparse
import json
import re
import sys
from urllib.parse import urlparse

from databricks.sdk import WorkspaceClient


def run_sql(w, warehouse_id: str, statement: str):
    """Execute a SQL statement via the statement execution API."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        statement=statement,
        wait_timeout="30s",
    )
    if resp.status and resp.status.error:
        print(f"  SQL error: {resp.status.error.message}")
        return None
    if resp.manifest and resp.result and resp.result.data_array:
        cols = [c.name for c in resp.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in resp.result.data_array]
    return []


def grant_uc_permissions(w, catalog: str, player: str, warehouse_id: str):
    """Grant Unity Catalog permissions via SQL (most reliable for all object types)."""
    print("\n[1/5] Unity Catalog grants")

    grants = [
        (f"GRANT USE CATALOG ON CATALOG {catalog} TO `{player}`", "USE CATALOG"),
    ]
    for schema in ["game", "lakeflow", "simulator", "menu_documents"]:
        grants.append((f"GRANT USE SCHEMA ON SCHEMA {catalog}.{schema} TO `{player}`", f"USE SCHEMA on {schema}"))
        grants.append((f"GRANT SELECT ON SCHEMA {catalog}.{schema} TO `{player}`", f"SELECT on {schema}"))

    grants.append((f"GRANT READ VOLUME ON VOLUME {catalog}.menu_documents.menus TO `{player}`", "READ VOLUME on menus"))

    for sql, label in grants:
        try:
            run_sql(w, warehouse_id, sql)
            print(f"  + {label}")
        except Exception as e:
            print(f"  ! {label}: {e}")


def grant_warehouse_access(w, warehouse_id: str, player: str):
    """Grant CAN_USE on the SQL warehouse."""
    print("\n[2/5] SQL warehouse access")
    try:
        perms = w.permissions.get(
            request_object_type="sql/warehouses",
            request_object_id=warehouse_id,
        )
        already = any(
            acl.user_name == player
            for acl in (perms.access_control_list or [])
        )
        if already:
            print(f"  ~ Player already has warehouse access")
            return

        _patch_permissions(w, "sql/warehouses", warehouse_id, player, "CAN_USE")
        print(f"  + CAN_USE on warehouse {warehouse_id}")
    except Exception as e:
        print(f"  ! Warehouse permission: {e}")


def _patch_permissions(w, object_type: str, object_id: str, player: str, level: str):
    """PATCH permissions using the raw SDK API client (avoids SDK object serialization issues)."""
    w.api_client.do(
        "PATCH",
        f"/api/2.0/permissions/{object_type}/{object_id}",
        body={"access_control_list": [{"user_name": player, "permission_level": level}]},
    )


def share_genie_room(w, catalog: str, player: str, warehouse_id: str):
    """Share the Genie room with the player."""
    print("\n[3/5] Genie room access")
    try:
        rows = run_sql(w, warehouse_id,
            f"SELECT config_value FROM {catalog}.game.config WHERE config_key = 'genie_room_url'")
        if not rows or not rows[0].get("config_value"):
            print("  - No Genie room URL found in game.config, skipping")
            return

        url = rows[0]["config_value"]
        match = re.search(r'/rooms/([^/?#]+)', url)
        if not match:
            print(f"  - Could not parse space_id from URL: {url}")
            return
        space_id = match.group(1)

        try:
            _patch_permissions(w, "genie", space_id, player, "CAN_RUN")
            print(f"  + Shared Genie room {space_id} with CAN_RUN")
        except Exception as e:
            print(f"  ! Genie room permission failed: {e}")
            print(f"    Manual step: Share Genie room with {player}")
    except Exception as e:
        print(f"  ! Genie room: {e}")


def share_dashboard(w, catalog: str, player: str, warehouse_id: str):
    """Share the published dashboard with the player."""
    print("\n[4/5] Dashboard access")
    try:
        rows = run_sql(w, warehouse_id,
            f"SELECT config_value FROM {catalog}.game.config WHERE config_key = 'dashboard_url'")
        if not rows or not rows[0].get("config_value"):
            print("  - No dashboard URL found in game.config, skipping")
            return

        url = rows[0]["config_value"]
        match = re.search(r'dashboardsv3/([^/?#/]+)', url)
        if not match:
            print(f"  - Could not parse dashboard_id from URL: {url}")
            return
        dashboard_id = match.group(1)

        try:
            _patch_permissions(w, "dashboards", dashboard_id, player, "CAN_READ")
            print(f"  + Shared dashboard {dashboard_id} with CAN_READ")
        except Exception as e:
            print(f"  ! Dashboard permission failed: {e}")
            print(f"    Published dashboards with embedded credentials may already be accessible.")
            print(f"    If not, share manually: Dashboard -> Share -> Add {player}")
    except Exception as e:
        print(f"  ! Dashboard: {e}")


def share_knowledge_assistant(w, catalog: str, player: str):
    """Share the Knowledge Assistant with the player."""
    print("\n[5/5] Knowledge Assistant access")
    ka_name = f"{catalog}-menu-knowledge"
    try:
        tiles_resp = w.api_client.do("GET", "/api/2.0/tiles")
        tile_id = None
        for tile in tiles_resp.get("tiles", []):
            if tile.get("name") == ka_name:
                tile_id = tile.get("tile_id")
                break

        if not tile_id:
            print(f"  - KA '{ka_name}' not found in tiles, skipping")
            return

        try:
            _patch_permissions(w, "agent-bricks", tile_id, player, "CAN_QUERY")
            print(f"  + Shared KA '{ka_name}' (tile {tile_id}) with CAN_QUERY")
        except Exception as e:
            print(f"  ! Could not share KA: {e}")
            print(f"    Manual step: Agent Bricks -> '{ka_name}' -> Share -> Add {player}")
    except Exception as e:
        print(f"  ! Knowledge Assistant: {e}")


def find_warehouse(w, catalog: str) -> str:
    """Find the SQL warehouse used by the game."""
    game_wh_name = f"{catalog}-game-warehouse"
    fallback_wh_name = f"{catalog}-warehouse"
    for wh in w.warehouses.list():
        if wh.name in (game_wh_name, fallback_wh_name):
            return wh.id
    # Fall back to any serverless warehouse
    for wh in w.warehouses.list():
        if wh.enable_serverless_compute:
            return wh.id
    # Last resort
    warehouses = list(w.warehouses.list())
    if warehouses:
        return warehouses[0].id
    print("ERROR: No SQL warehouse found.")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Grant a player all permissions to play Casper's Kitchen Rescue"
    )
    parser.add_argument("player_email", help="Email of the player to grant access to")
    parser.add_argument("--catalog", default="caspersdev", help="UC catalog (default: caspersdev)")
    parser.add_argument("--profile", default=None, help="Databricks CLI profile from ~/.databrickscfg")
    args = parser.parse_args()

    player = args.player_email
    catalog = args.catalog

    print(f"Casper's Kitchen Rescue — Grant Player Access")
    print(f"  Player:  {player}")
    print(f"  Catalog: {catalog}")

    w = WorkspaceClient(profile=args.profile) if args.profile else WorkspaceClient()
    print(f"  Host:    {w.config.host}")

    warehouse_id = find_warehouse(w, catalog)
    print(f"  Warehouse: {warehouse_id}")

    grant_uc_permissions(w, catalog, player, warehouse_id)
    grant_warehouse_access(w, warehouse_id, player)
    share_genie_room(w, catalog, player, warehouse_id)
    share_dashboard(w, catalog, player, warehouse_id)
    share_knowledge_assistant(w, catalog, player)

    print(f"\nDone! {player} should now be able to play the game.")
    print(f"Send them the quest controller app URL to get started.")


if __name__ == "__main__":
    main()
