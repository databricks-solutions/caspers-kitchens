#!/usr/bin/env bash
# Wrapper for cleanup that passes catalog to the script.
# Usage: ./cleanup.sh [catalog]
# Example: ./cleanup.sh oleksandra
# The Databricks CLI does not pass --var to script env, so we set BUNDLE_VAR_catalog.
CATALOG="${1:-caspersdev}"
export BUNDLE_VAR_catalog="$CATALOG"
exec databricks bundle run cleanup
