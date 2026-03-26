"""
Preview a query by running it on Dune and displaying the first 20 rows.

Finds the SQL file matching the given query ID, wraps it in a LIMIT 20
subquery, and executes it via the Dune API. Uses API credits.

Usage:
    python scripts/preview_query.py <query_id>
"""

from __future__ import annotations

import os
import sys

import pandas as pd
from dune_client.client import DuneClient
from dotenv import load_dotenv

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
dotenv_path = os.path.join(REPO_ROOT, ".env")
load_dotenv(dotenv_path)


def find_query_file(query_id: str, search_root: str) -> str | None:
    suffix = f"_{query_id}.sql"
    for dirpath, _, filenames in os.walk(search_root):
        for f in filenames:
            if f.endswith(suffix):
                return os.path.join(dirpath, f)
    return None


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/preview_query.py <query_id>")
        sys.exit(1)

    query_id = sys.argv[1]
    dune = DuneClient.from_env()

    file_path = find_query_file(query_id, REPO_ROOT)
    if not file_path:
        print(f"No file found for query ID {query_id}")
        sys.exit(1)

    print(f"Previewing query {query_id} from {file_path}...")

    with open(file_path, "r", encoding="utf-8") as f:
        query_text = f.read()

    wrapped = f"SELECT * FROM (\n{query_text}\n) LIMIT 20"
    print(f"\n{wrapped}\n")

    results = dune.run_sql(wrapped)
    df = pd.DataFrame(data=results.result.rows)

    print(df.to_string())
    print(f"\n{df.describe()}")
    print(f"\n{df.info()}")


if __name__ == "__main__":
    main()
