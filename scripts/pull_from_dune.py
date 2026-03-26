"""
Fetch queries from Dune into the local repository.

Reads query IDs from queries.yml, fetches each query's SQL and metadata
via the dune-client SDK, and writes them into the appropriate directory
with the standard header and naming convention.

Usage:
    python scripts/pull_from_dune.py
"""

from __future__ import annotations

import os
import re
import sys
import codecs

import yaml
from dune_client.client import DuneClient
from dotenv import load_dotenv

sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
dotenv_path = os.path.join(REPO_ROOT, ".env")
load_dotenv(dotenv_path)

HEADER_MARKER = "-- part of a query repo"
QUERIES_YML = os.path.join(REPO_ROOT, "queries.yml")


def sanitize_name(name: str, max_len: int = 60) -> str:
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9]+", "_", name)
    name = name.strip("_")
    return name[:max_len]


def find_existing_file(query_id: int, search_root: str) -> str | None:
    """Walk the repo looking for a file ending with _{query_id}.sql."""
    suffix = f"_{query_id}.sql"
    for dirpath, _, filenames in os.walk(search_root):
        for f in filenames:
            if f.endswith(suffix):
                return os.path.join(dirpath, f)
    return None


def build_header(name: str, query_id: int) -> str:
    return (
        f"{HEADER_MARKER}\n"
        f"-- query name: {name}\n"
        f"-- query link: https://dune.com/queries/{query_id}\n\n\n"
    )


def main():
    dune = DuneClient.from_env()

    with open(QUERIES_YML, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    entries = data.get("query_ids", [])
    if not entries:
        print("No query IDs found in queries.yml")
        return

    for entry in entries:
        if isinstance(entry, dict):
            qid = entry["id"]
            category = entry.get("category", "")
        else:
            qid = int(entry)
            category = ""

        query = dune.get_query(qid)
        print(f"PROCESSING: query {query.base.query_id}, {query.base.name}")

        if HEADER_MARKER in query.sql and category == "":
            print(f"  WARNING: query {qid} may already be managed in another repo")

        existing = find_existing_file(qid, REPO_ROOT)

        if existing:
            print(f"  UPDATE: {existing}")
            with open(existing, "w", encoding="utf-8") as f:
                if HEADER_MARKER in query.sql:
                    f.write(query.sql)
                else:
                    f.write(build_header(query.base.name, qid) + query.sql)
        else:
            fname = f"{sanitize_name(query.base.name)}_{qid}.sql"
            if category:
                target_dir = os.path.join(REPO_ROOT, "balancer", category)
            else:
                target_dir = os.path.join(REPO_ROOT, "balancer")
            os.makedirs(target_dir, exist_ok=True)
            file_path = os.path.join(target_dir, fname)

            print(f"  CREATE: {file_path}")
            with open(file_path, "w", encoding="utf-8") as f:
                if HEADER_MARKER in query.sql:
                    f.write(
                        f"-- WARNING: this query may be part of multiple repos\n{query.sql}"
                    )
                else:
                    f.write(build_header(query.base.name, qid) + query.sql)

    print(f"\nDone. Processed {len(entries)} queries.")


if __name__ == "__main__":
    main()
