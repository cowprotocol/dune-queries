"""
Validate the repository for consistency.

Checks:
  1. Every SQL file matches the naming convention *_{queryId}.sql
  2. Every SQL file has the "-- part of a query repo" header
  3. Every query ID in filenames is present in queries.yml (and vice versa)
  4. Query dependency graph: all query_{id} references resolve to files in the repo
  5. No duplicate query IDs across files

Usage:
    python scripts/validate.py
"""

import os
import re
import sys

import yaml

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
QUERIES_YML = os.path.join(REPO_ROOT, "queries.yml")
HEADER_MARKER = "-- part of a query repo"
FILENAME_PATTERN = re.compile(r"^.+_(\d+)\.sql$")
DEPENDENCY_PATTERN = re.compile(r"query_(\d+)")


def collect_sql_files(root: str) -> list[tuple[str, str, int]]:
    """Return list of (full_path, filename, query_id) for all .sql files."""
    results = []
    for dirpath, _, filenames in os.walk(root):
        if ".git" in dirpath or "scripts" in dirpath:
            continue
        for f in filenames:
            if not f.endswith(".sql"):
                continue
            full = os.path.join(dirpath, f)
            match = FILENAME_PATTERN.match(f)
            qid = int(match.group(1)) if match else -1
            results.append((full, f, qid))
    return results


def load_manifest_ids() -> set[int]:
    if not os.path.exists(QUERIES_YML):
        return set()
    with open(QUERIES_YML, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    entries = data.get("query_ids", [])
    ids = set()
    for e in entries:
        if isinstance(e, dict):
            ids.add(int(e["id"]))
        else:
            ids.add(int(e))
    return ids


def main():
    sql_files = collect_sql_files(REPO_ROOT)
    manifest_ids = load_manifest_ids()
    errors = []
    warnings = []

    file_ids: dict[int, list[str]] = {}
    all_dependencies: dict[str, set[int]] = {}

    for full_path, filename, qid in sql_files:
        rel_path = os.path.relpath(full_path, REPO_ROOT)

        if qid == -1:
            errors.append(f"BAD FILENAME: {rel_path} does not match *_{{queryId}}.sql")
            continue

        file_ids.setdefault(qid, []).append(rel_path)

        with open(full_path, "r", encoding="utf-8") as f:
            content = f.read()

        if HEADER_MARKER not in content:
            warnings.append(f"MISSING HEADER: {rel_path} lacks '{HEADER_MARKER}'")

        deps = set(int(m) for m in DEPENDENCY_PATTERN.findall(content))
        deps.discard(qid)
        if deps:
            all_dependencies[rel_path] = deps

    all_file_ids = set(file_ids.keys())

    for qid, paths in file_ids.items():
        if len(paths) > 1:
            errors.append(f"DUPLICATE ID {qid}: {', '.join(paths)}")

    if manifest_ids:
        in_files_not_manifest = all_file_ids - manifest_ids
        in_manifest_not_files = manifest_ids - all_file_ids

        for qid in sorted(in_files_not_manifest):
            warnings.append(
                f"FILE NOT IN MANIFEST: query {qid} exists as file but not in queries.yml"
            )
        for qid in sorted(in_manifest_not_files):
            errors.append(
                f"MANIFEST NOT IN FILES: query {qid} is in queries.yml but no file found"
            )

    for rel_path, deps in all_dependencies.items():
        missing = deps - all_file_ids
        for dep_id in sorted(missing):
            warnings.append(
                f"UNRESOLVED DEP: {rel_path} references query_{dep_id} (not in repo)"
            )

    print(f"Scanned {len(sql_files)} SQL files.\n")

    if errors:
        print(f"ERRORS ({len(errors)}):")
        for e in errors:
            print(f"  {e}")
        print()

    if warnings:
        print(f"WARNINGS ({len(warnings)}):")
        for w in warnings:
            print(f"  {w}")
        print()

    if not errors and not warnings:
        print("All checks passed.")

    sys.exit(1 if errors else 0)


if __name__ == "__main__":
    main()
