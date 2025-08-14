#!/usr/bin/env python3
import argparse
import csv
import json
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets.readonly",
          "https://www.googleapis.com/auth/drive.readonly"]

def parse_args():
  ap = argparse.ArgumentParser(description="Fetch a Google Sheet tab and save as CSV/JSON.")
  ap.add_argument("--spreadsheet-id", required=True)
  ap.add_argument("--sheet-name", required=True, help="Tab name (as shown in the Google Sheet)")
  ap.add_argument("--out-path", required=True, help="Destination file in repo (e.g., data/sheets/output.csv)")
  ap.add_argument("--out-format", default="csv", choices=["csv", "json"])
  ap.add_argument("--sa-key-file", required=True, help="Path to service account JSON key")
  return ap.parse_args()

def get_values(spreadsheet_id, sheet_name, creds):
  service = build("sheets", "v4", credentials=creds, cache_discovery=False)
  rng = f"{sheet_name}"  # whole tab
  result = service.spreadsheets().values().get(
      spreadsheetId=spreadsheet_id,
      range=rng,
      valueRenderOption="UNFORMATTED_VALUE"
  ).execute()
  return result.get("values", []) or []

def write_csv(values, out_path):
  Path(out_path).parent.mkdir(parents=True, exist_ok=True)
  with open(out_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    for row in values:
      writer.writerow([("" if v is None else v) for v in row])
  print(f"✅ Wrote CSV: {out_path}")

def write_json(values, out_path):
  Path(out_path).parent.mkdir(parents=True, exist_ok=True)
  # If first row looks like headers, use it; otherwise write raw rows
  if values:
    headers = [str(h).strip() if h is not None else "" for h in values[0]]
    rows = values[1:]
    objs = []
    for r in rows:
      obj = {}
      for i, h in enumerate(headers):
        key = h or f"col_{i+1}"
        obj[key] = r[i] if i < len(r) else None
      objs.append(obj)
  else:
    objs = []

  with open(out_path, "w", encoding="utf-8") as f:
    json.dump(objs, f, ensure_ascii=False, indent=2)
  print(f"✅ Wrote JSON: {out_path}")

def main():
  args = parse_args()

  # Load service account credentials
  with open(args.sa_key_file, "r", encoding="utf-8") as f:
    info = json.load(f)
  creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

  # Fetch data
  values = get_values(args.spreadsheet_id, args.sheet_name, creds)

  # Save
  if args.out_format.lower() == "csv":
    write_csv(values, args.out_path)
  else:
    write_json(values, args.out_path)

if __name__ == "__main__":
  main()
