"""
Push all managed queries from the repository to Dune.

Reads query IDs from queries.yml, finds matching local SQL files,
and updates Dune with the file contents. This is a manual full-sync
alternative to the CI-based bh2smith/dune-update action.

Usage:
    python scripts/push_to_dune.py
"""

from __future__ import annotations

import os
import sys
import codecs

import yaml
from dune_client.client import DuneClient
from dotenv import load_dotenv

sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
dotenv_path = os.path.join(REPO_ROOT, ".env")
load_dotenv(dotenv_path)

QUERIES_YML = os.path.join(REPO_ROOT, "queries.yml")


def find_query_file(query_id: int, search_root: str) -> str | None:
    suffix = f"_{query_id}.sql"
    for dirpath, _, filenames in os.walk(search_root):
        for f in filenames:
            if f.endswith(suffix):
                return os.path.join(dirpath, f)
    return None


def main():
    dune = DuneClient.from_env()

    with open(QUERIES_YML, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)

    entries = data.get("query_ids", [])
    if not entries:
        print("No query IDs found in queries.yml")
        return

    success, errors = 0, 0
    for entry in entries:
        qid = entry["id"] if isinstance(entry, dict) else int(entry)

        file_path = find_query_file(qid, REPO_ROOT)
        if not file_path:
            print(f"ERROR: no file found for query {qid}")
            errors += 1
            continue

        with open(file_path, "r", encoding="utf-8") as f:
            sql_text = f.read()

        dune.update_query(qid, query_sql=sql_text)
        print(f"SUCCESS: pushed query {qid} from {file_path}")
        success += 1

    print(f"\nDone. {success} pushed, {errors} errors.")


if __name__ == "__main__":
    main()
