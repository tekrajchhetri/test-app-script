#!/usr/bin/env python3
import argparse
import json
import os
import sys
from datetime import datetime

def parse_args():
    ap = argparse.ArgumentParser(description="Handle Google Sheets event from workflow_dispatch.")
    ap.add_argument("--sheet-name", default="")
    ap.add_argument("--spreadsheet-id", default="")
    ap.add_argument("--spreadsheet-name", default="")
    ap.add_argument("--change-type", default="")
    ap.add_argument("--edited-a1", default="")
    ap.add_argument("--actor-email", default="")
    ap.add_argument("--source", default="google-sheets")
    return ap.parse_args()

def main():
    args = parse_args()

    # Example: turn inputs into a record
    event = {
        "sheet_name": args.sheet_name,
        "spreadsheet_id": args.spreadsheet_id,
        "spreadsheet_name": args.spreadsheet_name,
        "change_type": args.change_type,
        "edited_a1": args.edited_a1,
        "actor_email": args.actor_email,
        "source": args.source,
        "received_at": datetime.utcnow().isoformat() + "Z",
        "workflow_run_id": os.getenv("GITHUB_RUN_ID"),
        "workflow_repo": os.getenv("GITHUB_REPOSITORY"),
        "workflow_sha": os.getenv("GITHUB_SHA"),
    }

    # Do your thing:
    # - Pull the latest data from Google Sheets (if you have creds)
    # - Transform to CSV/JSON/Parquet
    # - Validate & run tests
    # - Commit or open PR
    # - Upload artifact, etc.

    # For demo: write the event to a file
    os.makedirs("artifacts", exist_ok=True)
    out_path = "artifacts/sheets_event.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(event, f, indent=2)
    print(f"Wrote {out_path}")
    print(json.dumps(event, indent=2))

    # If something fails, exit non-zero to fail the job:
    # sys.exit(1)

if __name__ == "__main__":
    main()
