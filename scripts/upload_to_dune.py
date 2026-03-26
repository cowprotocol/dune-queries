"""
Upload CSV files from the /uploads directory to Dune as tables.

Each CSV file becomes a table named dune.{team_name}.dataset_{filename}
(without the .csv extension, lowercased, spaces replaced with underscores).

Usage:
    python scripts/upload_to_dune.py
"""

import os
import sys
import codecs

from dune_client.client import DuneClient
from dotenv import load_dotenv

sys.stdout = codecs.getwriter("utf-8")(sys.stdout.detach())

REPO_ROOT = os.path.join(os.path.dirname(__file__), "..")
dotenv_path = os.path.join(REPO_ROOT, ".env")
load_dotenv(dotenv_path)

UPLOADS_DIR = os.path.join(REPO_ROOT, "uploads")


def main():
    dune = DuneClient.from_env()

    if not os.path.isdir(UPLOADS_DIR):
        print("No uploads/ directory found.")
        return

    files = [f for f in os.listdir(UPLOADS_DIR) if f.endswith(".csv")]
    if not files:
        print("No CSV files in uploads/.")
        return

    for filename in files:
        table_name = os.path.splitext(filename)[0].lower().replace(" ", "_")
        file_path = os.path.join(UPLOADS_DIR, filename)

        with open(file_path, "r", encoding="utf-8") as f:
            data = f.read()

        dune.upload_csv(data=data, table_name=table_name, is_private=False)
        print(f'Uploaded table "{table_name}" from {filename}')

    print(f"\nDone. Uploaded {len(files)} tables.")


if __name__ == "__main__":
    main()
