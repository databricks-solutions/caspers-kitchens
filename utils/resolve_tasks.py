#!/usr/bin/env python3
import re, sys, pathlib, yaml
from collections import OrderedDict, deque

ROOT = pathlib.Path(__file__).resolve().parents[1]
BUNDLE_FILE = ROOT / "databricks.yml"
JOB_KEY = "caspers"  # resources.jobs.<JOB_KEY>

def load_job():
    try:
        data = yaml.safe_load(BUNDLE_FILE.read_text())
    except FileNotFoundError:
        raise SystemExit(f"databricks.yml not found at {BUNDLE_FILE}")
    except Exception as e:
        raise SystemExit(f"Failed to parse YAML: {e}")

    job = (
        data.get("resources", {})
            .get("jobs", {})
            .get(JOB_KEY)
    )
    if not job:
        raise SystemExit("Could not find resources.jobs.caspers in databricks.yml")
    tasks = job.get("tasks", [])
    if not isinstance(tasks, list) or not tasks:
        raise SystemExit("resources.jobs.caspers.tasks is missing or empty")
    return tasks

def build_revdeps(tasks):
    """Return (task_keys, reverse_deps) where reverse_deps[k] = list of parents of k."""
    keys = []
    rev = {}
    for t in tasks:
        k = t.get("task_key")
        if not k:
            raise SystemExit("A task is missing task_key")
        keys.append(k)
    # map parents
    for t in tasks:
        k = t["task_key"]
        parents = [d["task_key"] for d in t.get("depends_on", [])] if t.get("depends_on") else []
        for p in parents:
            if p not in keys:
                raise SystemExit(f"Unknown dependency {p} for task {k}")
        rev[k] = parents
    return keys, rev

def resolve_with_parents(targets, revdeps):
    """Include all ancestors and return CSV ordered parent→child."""
    seen = OrderedDict()
    q = deque(targets)
    while q:
        cur = q.popleft()
        if cur in seen:
            continue
        seen[cur] = True
        for p in revdeps.get(cur, []):
            q.append(p)

    depth_cache = {}
    def depth(n):
        if n in depth_cache:
            return depth_cache[n]
        parents = revdeps.get(n, [])
        d = 0 if not parents else 1 + max(depth(p) for p in parents)
        depth_cache[n] = d
        return d

    ordered = sorted(seen.keys(), key=lambda n: (depth(n), n))
    return ",".join(ordered)

def safe_varname(name):
    """Ensure shell-safe env var; warn if altered (shouldn’t be needed for your keys)."""
    if re.match(r'^[A-Za-z_][A-Za-z0-9_]*$', name):
        return name, None
    safe = re.sub(r'[^A-Za-z0-9_]', '_', name)
    return safe, f"Warning: task_key '{name}' is not a valid shell variable. Using '{safe}'."

def main():
    tasks = load_job()
    keys, rev = build_revdeps(tasks)

    print('# source this:  source <(python utils/resolve_tasks.py)')
    # Export one var per task key with resolved CSV
    for k in sorted(keys):
        var, warn = safe_varname(k)
        if warn:
            print(f'echo "{warn}" >&2')
        csv = resolve_with_parents([k], rev)
        print(f'export {var}="{csv}"')

    # friendly hint
    print('echo "Casper\'s task shortcuts loaded. Example: databricks bundle run caspers --only \\$Refund_Recommender_Agent"')

if __name__ == "__main__":
    main()
