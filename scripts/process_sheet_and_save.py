# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# DISCLAIMER: This software is provided "as is" without any warranty,
# express or implied, including but not limited to the warranties of
# merchantability, fitness for a particular purpose, and non-infringement.
#
# In no event shall the authors or copyright holders be liable for any
# claim, damages, or other liability, whether in an action of contract,
# tort, or otherwise, arising from, out of, or in connection with the
# software or the use or other dealings in the software.
# -----------------------------------------------------------------------------

# @Author  : Tek Raj Chhetri
# @Email   : tekraj@mit.edu
# @Web     : https://tekrajchhetri.com/
# @File    : tests.py
# @Software: PyCharm

#!/usr/bin/env python3

import argparse
import csv
import json
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

def map_columns_to_labels(columns):
    mapping = {
        "What do you do?": "Role",
        "What knowledge would you like to share?": "Expertise",
        "What would you like to learn?": "Interest",
        "What additional would you like to share?": "Note",
        "Submitted By": "Name",
        "Timestamp": "Time",
    }
    return [mapping.get(col, col) for col in columns]

def parse_args():
    ap = argparse.ArgumentParser(
        description="Fetch a Google Sheet tab and append to CSV/JSONL (dedupe by submitter)."
    )
    ap.add_argument("--spreadsheet-id", required=True)
    ap.add_argument("--sheet-name", required=True, help="Tab name (as shown in the Google Sheet)")
    ap.add_argument("--out-path", required=True, help="Destination file (e.g., data/sheets/output.csv or .jsonl)")
    ap.add_argument("--out-format", default="csv", choices=["csv", "json"], help="'json' outputs JSON Lines (NDJSON)")
    ap.add_argument("--sa-key-file", required=True, help="Path to service account JSON key")
    return ap.parse_args()

def get_values(spreadsheet_id, sheet_name, creds):
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}",
        valueRenderOption="UNFORMATTED_VALUE"
    ).execute()
    return result.get("values", []) or []

def _norm(s):
    return ("" if s is None else str(s)).strip()

def _norm_key(s):
    return _norm(s).lower()

def _find_header_index(headers, names):
    """Find the first index of any header name (case-insensitive)."""
    targets = { _norm_key(n) for n in (names if isinstance(names, (list,tuple,set)) else [names]) }
    for i, h in enumerate(headers or []):
        if _norm_key(h) in targets:
            return i
    return None

def _existing_submitter_keys_csv(out_path):
    """Collect existing submitter keys from CSV (Name or Submitted By)."""
    keys = set()
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        return keys
    with open(out_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            return keys
        idx = _find_header_index(headers, ["Name", "Submitted By"])
        if idx is None:
            return keys
        for row in reader:
            if len(row) > idx:
                keys.add(_norm_key(row[idx]))
    return keys

def _existing_submitter_keys_jsonl(out_path):
    """Collect existing submitter keys from NDJSON (Name or Submitted By)."""
    keys = set()
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        return keys
    with open(out_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                key = obj.get("Name") or obj.get("Submitted By") or ""
                keys.add(_norm_key(key))
            except Exception:
                # Skip malformed lines
                continue
    return keys

def write_csv_append(values, out_path):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if not values:
        print("No values to write.")
        return

    # Map headers
    raw_headers = [ _norm(h) for h in values[0] ]
    headers = map_columns_to_labels(raw_headers)
    rows = values[1:]

    # Build incoming rows aligned to mapped headers as we don't want questions
    header_idx = { _norm_key(h): i for i, h in enumerate(headers) }
    # Identify submitter index from original headers (pre-map) OR mapped headers
    submitted_idx_raw = _find_header_index(raw_headers, ["Submitted By", "Name"])
    submitted_idx_mapped = _find_header_index(headers, ["Name", "Submitted By"])
    submitted_idx = submitted_idx_raw if submitted_idx_raw is not None else submitted_idx_mapped

    existing_keys = _existing_submitter_keys_csv(out_path)

    # Prepare to write in append mode
    file_exists = os.path.exists(out_path)
    is_empty = (not file_exists) or os.path.getsize(out_path) == 0

    # If empty, write headers first
    with open(out_path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        if is_empty:
            w.writerow(headers)

        added, skipped = 0, 0
        for r in rows:
            # Build mapped row by position (pad/truncate)
            mapped_row = []
            for i in range(len(headers)):
                mapped_row.append(_norm(r[i]) if i < len(r) else "")
            # Dedup by submitter
            key = _norm_key(r[submitted_idx]) if (submitted_idx is not None and submitted_idx < len(r)) else ""
            if key and key not in existing_keys:
                w.writerow(mapped_row)
                existing_keys.add(key)
                added += 1
            else:
                skipped += 1

    print(f"CSV append: {out_path} | added: {added} | skipped: {skipped}")

def write_jsonl_append(values, out_path):
    """Append as JSON Lines (NDJSON), one object per line, with mapped headers."""
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if not values:
        print("No values to write.")
        return

    raw_headers = [ _norm(h) for h in values[0] ]
    headers = map_columns_to_labels(raw_headers)
    rows = values[1:]

    existing_keys = _existing_submitter_keys_jsonl(out_path)

    added, skipped = 0, 0
    with open(out_path, "a", encoding="utf-8") as f:
        for r in rows:
            obj = {}
            for i, h in enumerate(headers):
                key = h if h else f"col_{i+1}"
                obj[key] = _norm(r[i]) if i < len(r) else None
            submit_key = _norm_key(obj.get("Name") or obj.get("Submitted By") or "")
            if submit_key and submit_key not in existing_keys:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
                existing_keys.add(submit_key)
                added += 1
            else:
                skipped += 1

    print(f"JSONL append: {out_path} | added: {added} | skipped: {skipped}")

def main():
    args = parse_args()

    # Load service account credentials
    with open(args.sa_key_file, "r", encoding="utf-8") as f:
        info = json.load(f)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    # Fetch data
    values = get_values(args.spreadsheet_id, args.sheet_name, creds)

    # Save (append-only + dedupe by submitter)
    fmt = args.out_format.lower()
    if fmt == "csv":
        write_csv_append(values, args.out_path)
    else:
        # 'json' means NDJSON append for true append semantics
        write_jsonl_append(values, args.out_path)

if __name__ == "__main__":
    main()
