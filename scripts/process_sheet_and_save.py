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
import time
from pathlib import Path
from typing import Dict, Any, Optional

import requests
from google.oauth2 import service_account
from googleapiclient.discovery import build

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.readonly",
]

# ---------------- Column mapping ----------------
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
        description="Fetch a Google Sheet and append: mapped original to CSV; transformed to NDJSON."
    )
    ap.add_argument("--spreadsheet-id", required=True)
    ap.add_argument("--sheet-name", required=True)
    ap.add_argument("--csv-out", required=True, help="Path for CSV (mapped headers, append)")
    ap.add_argument("--json-out", required=True, help="Path for NDJSON (with mappings)")
    ap.add_argument("--sa-key-file", required=True)

    # LLM mapping toggle: default True, but allow --no-llm-mapping to disable
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--enable-llm-mapping", dest="enable_llm_mapping", action="store_true",
                     help="Enable ontology mapping with OpenRouter (default)")
    grp.add_argument("--no-llm-mapping", dest="enable_llm_mapping", action="store_false",
                     help="Disable ontology mapping with OpenRouter")
    ap.set_defaults(enable_llm_mapping=True)

    ap.add_argument("--openrouter-model", default="openai/gpt-4o-mini",
                    help="OpenRouter model id (e.g., openai/gpt-4o-mini)")
    ap.add_argument("--openrouter-base-url", default="https://openrouter.ai/api/v1/chat/completions")
    ap.add_argument("--openrouter-timeout", type=int, default=60)
    ap.add_argument("--openrouter-sleep", type=float, default=0.0,
                    help="Sleep seconds between LLM calls (rate limiting)")
    return ap.parse_args()

# ---------------- Google Sheets ----------------
def get_values(spreadsheet_id, sheet_name, creds):
    service = build("sheets", "v4", credentials=creds, cache_discovery=False)
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}",
        valueRenderOption="UNFORMATTED_VALUE"
    ).execute()
    return result.get("values", []) or []

# ---------------- Utils ----------------
def _norm(s): return ("" if s is None else str(s)).strip()
def _norm_key(s): return _norm(s).lower()

def _find_header_index(headers, names):
    targets = {_norm_key(n) for n in (names if isinstance(names, (list, tuple, set)) else [names])}
    for i, h in enumerate(headers or []):
        if _norm_key(h) in targets:
            return i
    return None

def _existing_keys_csv(out_path):
    keys = set()
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        return keys
    with open(out_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keys.add(_norm_key(row.get("Name") or row.get("Submitted By") or ""))
    return keys

def _existing_keys_jsonl(out_path):
    keys = set()
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        return keys
    with open(out_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
                keys.add(_norm_key(obj.get("fields", {}).get("Name") or ""))
            except Exception:
                continue
    return keys

# ---------------- OpenRouter LLM Mapping ----------------
def _llm_system_prompt():
    return (
        "You are an ontology mapping assistant. Given Role, Expertise, and Interest, "
        "map each to an ontological concept from public ontologies (e.g., ESCO for roles, OBO/NCIT/MeSH for biomedical). "
        "Return strict JSON with keys Role, Expertise, Interest, each containing: "
        "{concept_label, ontology_id, ontology, confidence, explanation}. "
        "Use nulls for fields you cannot map. confidence is 0.0–1.0."
    )

def _llm_user_prompt(role, expertise, interest):
    return json.dumps({"Role": role or "", "Expertise": expertise or "", "Interest": interest or ""}, ensure_ascii=False)

def _call_openrouter(base_url, api_key, model, system_prompt, user_prompt, timeout):
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": model,
        "response_format": {"type": "json_object"},
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
    }
    try:
        resp = requests.post(base_url, headers=headers, json=payload, timeout=timeout)
        resp.raise_for_status()
        return json.loads(resp.json()["choices"][0]["message"]["content"])
    except Exception as e:
        print(f"⚠️ OpenRouter call failed: {e}")
        return None

def get_mappings(role, expertise, interest, cfg):
    api_key = os.getenv("OPENROUTER_API_KEY")  # GitHub secret
    if not api_key:
        print("ℹ️ OPENROUTER_API_KEY not set; skipping ontology mapping.")
        return None
    res = _call_openrouter(
        cfg["base_url"], api_key, cfg["model"],
        _llm_system_prompt(),
        _llm_user_prompt(role, expertise, interest),
        cfg["timeout"]
    )
    # Optional pacing for rate limits
    if cfg.get("sleep_s", 0):
        time.sleep(cfg["sleep_s"])
    return res

# ---------------- Writers ----------------
def append_csv(values, out_path):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if not values:
        return

    headers_raw = [_norm(h) for h in values[0]]
    headers_mapped = map_columns_to_labels(headers_raw)
    rows = values[1:]

    existing_keys = _existing_keys_csv(out_path)
    write_header = not os.path.exists(out_path) or os.path.getsize(out_path) == 0

    with open(out_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers_mapped)
        if write_header:
            writer.writeheader()
        for row in rows:
            row_dict = {headers_mapped[i]: _norm(row[i]) if i < len(row) else "" for i in range(len(headers_mapped))}
            key = _norm_key(row_dict.get("Name", ""))
            if key and key not in existing_keys:
                writer.writerow(row_dict)
                existing_keys.add(key)

def append_jsonl(values, out_path, llm_cfg, enable_mapping):
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if not values:
        return

    headers_raw = [_norm(h) for h in values[0]]
    headers_mapped = map_columns_to_labels(headers_raw)
    rows = values[1:]

    existing_keys = _existing_keys_jsonl(out_path)

    with open(out_path, "a", encoding="utf-8") as f:
        for row in rows:
            row_dict = {headers_mapped[i]: _norm(row[i]) if i < len(row) else "" for i in range(len(headers_mapped))}
            key = _norm_key(row_dict.get("Name", ""))
            if not key or key in existing_keys:
                continue

            obj = {
                "original": row_dict,
                "fields": row_dict.copy()
            }
            if enable_mapping:
                mappings = get_mappings(
                    row_dict.get("Role"), row_dict.get("Expertise"), row_dict.get("Interest"),
                    llm_cfg
                )
                if mappings:
                    obj["mappings"] = mappings

            f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            existing_keys.add(key)

def main():
    args = parse_args()
    with open(args.sa_key_file, "r", encoding="utf-8") as f:
        info = json.load(f)
    creds = service_account.Credentials.from_service_account_info(info, scopes=SCOPES)

    values = get_values(args.spreadsheet_id, args.sheet_name, creds)

    llm_cfg = {
        "model": args.openrouter_model,
        "base_url": args.openrouter_base_url,
        "timeout": args.openrouter_timeout,
        "sleep_s": args.openrouter_sleep
    }

    append_csv(values, args.csv_out)
    append_jsonl(values, args.json_out, llm_cfg, args.enable_llm_mapping)

if __name__ == "__main__":
    main()
