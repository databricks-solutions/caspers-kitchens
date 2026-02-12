#!/usr/bin/env python3
"""
Simple database migration runner.

Usage: 
    python migrate.py <connection_string>
    python migrate.py  # uses CONNECTION_STRING env var
    
Runs all .sql files in schema/migrations/ that haven't been applied yet.
Tracks applied migrations in public.schema_migrations table.
"""
import os
import sys
from pathlib import Path

import psycopg2

MIGRATIONS_DIR = Path(__file__).parent / "schema" / "migrations"


def migrate(dsn: str):
    print(f"Connecting to database...")
    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    cur = conn.cursor()

    # Create migrations tracking table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS public.schema_migrations (
            version TEXT PRIMARY KEY,
            applied_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # Get already applied migrations
    cur.execute("SELECT version FROM public.schema_migrations")
    applied = {row[0] for row in cur.fetchall()}
    print(f"Already applied: {len(applied)} migration(s)")

    # Find and run pending migrations in order
    migration_files = sorted(MIGRATIONS_DIR.glob("*.sql"))
    pending = [f for f in migration_files if f.name not in applied]

    if not pending:
        print("✓ Schema is up to date.")
        conn.close()
        return

    print(f"Pending: {len(pending)} migration(s)")
    
    for f in pending:
        print(f"\nApplying {f.name}...")
        sql = f.read_text()
        try:
            cur.execute(sql)
            cur.execute(
                "INSERT INTO public.schema_migrations (version) VALUES (%s)",
                (f.name,)
            )
            print(f"  ✓ Done")
        except Exception as e:
            print(f"  ✗ Failed: {e}")
            conn.close()
            sys.exit(1)

    print(f"\n✓ All migrations applied successfully.")
    conn.close()


if __name__ == "__main__":
    if len(sys.argv) > 1:
        dsn = sys.argv[1]
    else:
        dsn = os.environ.get("CONNECTION_STRING")
        if not dsn:
            print("Usage: python migrate.py <connection_string>")
            print("   or: CONNECTION_STRING=... python migrate.py")
            sys.exit(1)
    
    migrate(dsn)
