# -*- coding: utf-8 -*-
# -----------------------------------------------------------------------------
# DISCLAIMER: This software is provided "as is" without any warranty,
# express or implied, including but not limited to the warranties of
# merchantability, fitness for a particular purpose, and non-infringement.
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
from typing import Dict, Any, Optional, List

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
        description="Fetch a Google Sheet and append CSV; write pretty JSON and maintain a shared ontology-mapping cache."
    )
    ap.add_argument("--spreadsheet-id", required=True)
    ap.add_argument("--sheet-name", required=True)
    ap.add_argument("--csv-out", required=True, help="Path for CSV (mapped headers, append)")
    ap.add_argument("--json-out", required=True, help="Path for pretty JSON array (user entries)")
    ap.add_argument("--mappings-store", default=None,
                    help="Path for the shared mappings cache JSON (default: next to json-out as mappings_store.json)")
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

def _existing_keys_csv(out_path):
    keys = set()
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        return keys
    with open(out_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            keys.add(_norm_key(row.get("Name") or row.get("Submitted By") or ""))
    return keys

def _load_existing_json(out_path) -> List[Dict[str, Any]]:
    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        return []
    try:
        with open(out_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []

def _index_by_name(objs: List[Dict[str, Any]]):
    idx = {}
    for i, obj in enumerate(objs):
        name = _norm_key(obj.get("fields", {}).get("Name", ""))
        if name:
            idx[name] = i
    return idx

# --------------- Shared Mappings Store (cache) ----------------
def _default_store_path(json_out: str) -> str:
    p = Path(json_out)
    return str(p.with_name("mappings_store.json"))

def _load_store(path: str) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return {"Role": {}, "Expertise": {}, "Interest": {}}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            # ensure structure
            for k in ("Role", "Expertise", "Interest"):
                data.setdefault(k, {})
            return data
    except Exception:
        return {"Role": {}, "Expertise": {}, "Interest": {}}

def _save_store(path: str, store: Dict[str, Any]) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False, indent=2)

def _merge_mapping_list(existing_list: List[Dict[str, Any]], new_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Merge by ontology_id; keep higher confidence; dedupe explanations.
    Append brand-new ontology_ids.
    """
    by_id = { (m.get("ontology_id") or "", m.get("ontology") or ""): m for m in existing_list or [] }
    for m in new_list or []:
        key = (m.get("ontology_id") or "", m.get("ontology") or "")
        if key in by_id:
            cur = by_id[key]
            # Keep the higher confidence
            try:
                if (m.get("confidence") or 0) > (cur.get("confidence") or 0):
                    cur["confidence"] = m.get("confidence")
            except Exception:
                pass
            # Prefer non-null concept_label
            if not cur.get("concept_label") and m.get("concept_label"):
                cur["concept_label"] = m.get("concept_label")
            # Merge/shorten explanations
            ex_set = {e.strip() for e in [cur.get("explanation", ""), m.get("explanation", "")] if e}
            cur["explanation"] = "; ".join(sorted(ex_set)) if ex_set else None
        else:
            by_id[key] = {
                "concept_label": m.get("concept_label"),
                "ontology_id": m.get("ontology_id"),
                "ontology": m.get("ontology"),
                "confidence": m.get("confidence"),
                "explanation": m.get("explanation"),
            }
    return list(by_id.values())

# ---------------- OpenRouter LLM Mapping ----------------
def _llm_system_prompt():
    # NOTE: include source_term to key the shared cache
    prompt = """
You are an Ontology Mapping Assistant.

TASK:
1) Read Role, Expertise, and Interest (free-text).
2) Identify and map each relevant word/phrase to a public-ontology concept.
3) Return STRICT JSON with exactly keys: Role, Expertise, Interest.
   Each is a list of objects with the schema:
   {
     "source_term": str,                     // the exact word/phrase you mapped
     "concept_label": str|null,
     "ontology_id": str|null,
     "ontology": str|null,
     "confidence": float|null,               // [0.0, 1.0]
     "explanation": str|null
   }

RULES:
- Use only public ontologies (e.g., OBO, UMLS, Wikidata).
- Prefer preferred labels; avoid partial matches unless clearly best.
- If nothing maps, omit it or return a single object with all fields null (still include source_term).
- Keep outputs compact and valid JSON.

EXAMPLE (Interest only shown for brevity):
Input: {"Role":"","Expertise":"","Interest":"how the human social brain develops"}
Output: {
  "Role": [],
  "Expertise": [],
  "Interest": [
    {"source_term":"human","concept_label":"Human","ontology_id":"Wikidata:Q5","ontology":"Wikidata","confidence":0.9,"explanation":"Human species"},
    {"source_term":"brain","concept_label":"Brain","ontology_id":"Wikidata:Q1073","ontology":"Wikidata","confidence":0.9,"explanation":"Organ"}
  ]
}
"""
    return prompt

def _llm_user_prompt(role, expertise, interest):
    return json.dumps(
        {"Role": role or "", "Expertise": expertise or "", "Interest": interest or ""},
        ensure_ascii=False
    )

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
        print(f"OpenRouter call failed: {e}")
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

def _ensure_store_keys(store: Dict[str, Any]):
    for k in ("Role", "Expertise", "Interest"):
        store.setdefault(k, {})

def _update_store_with_llm(store: Dict[str, Any], llm_out: Dict[str, Any]):
    _ensure_store_keys(store)
    for cat in ("Role", "Expertise", "Interest"):
        items = llm_out.get(cat) or []
        # Group by source_term
        by_term: Dict[str, List[Dict[str, Any]]] = {}
        for m in items:
            term = _norm_key(m.get("source_term") or "")
            if not term:
                continue
            by_term.setdefault(term, []).append({
                "concept_label": m.get("concept_label"),
                "ontology_id": m.get("ontology_id"),
                "ontology": m.get("ontology"),
                "confidence": m.get("confidence"),
                "explanation": m.get("explanation"),
            })
        # Merge into store
        for term, new_list in by_term.items():
            existing_list = store[cat].get(term, [])
            store[cat][term] = _merge_mapping_list(existing_list, new_list)

def _snapshot_user_mappings_from_store(store: Dict[str, Any], fields: Dict[str, str]) -> Dict[str, Any]:
    """Lookup Role/Expertise/Interest strings as whole terms in the store and return snapshot mapping lists."""
    out = {"Role": [], "Expertise": [], "Interest": []}
    for cat in ("Role", "Expertise", "Interest"):
        term = _norm_key(fields.get(cat, ""))
        if not term:
            out[cat] = []
            continue
        # If the whole field isn't a key, try simple splits (fallback)
        if term in store.get(cat, {}):
            out[cat] = store[cat][term]
        else:
            # fallback heuristic: split on commas and "and"; then look up each chunk
            aggregates: List[Dict[str, Any]] = []
            chunks = [c.strip() for c in term.replace(" and ", ",").split(",") if c.strip()]
            seen = set()
            for ch in chunks:
                lst = store.get(cat, {}).get(ch, [])
                for m in lst:
                    key = (m.get("ontology_id") or "", m.get("ontology") or "")
                    if key not in seen:
                        seen.add(key)
                        aggregates.append(m)
            out[cat] = aggregates
    return out

def write_json_pretty(values, out_path, store_path, llm_cfg, enable_mapping):
    """
    Writes a pretty-printed JSON array of user entries.
    Each entry:
      {
        "fields": { ...mapped headers... },
        "mappings": { "Role": [...], "Expertise": [...], "Interest": [...] }  # pulled from cache; LLM only for misses
      }
    De-duplicates by fields.Name (case-insensitive).
    """
    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
    if not values:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        return

    headers_raw = [_norm(h) for h in values[0]]
    headers_mapped = map_columns_to_labels(headers_raw)
    rows = values[1:]

    # Load existing output and index
    existing_objs = _load_existing_json(out_path)
    name_index = _index_by_name(existing_objs)

    # Load mapping store (shared cache)
    if not store_path:
        store_path = _default_store_path(out_path)
    store = _load_store(store_path)

    for row in rows:
        row_dict = {headers_mapped[i]: _norm(row[i]) if i < len(row) else "" for i in range(len(headers_mapped))}
        key = _norm_key(row_dict.get("Name", ""))
        if not key:
            continue

        # Ensure an entry exists (insert or update fields)
        if key in name_index:
            obj = existing_objs[name_index[key]]
            obj["fields"] = row_dict
        else:
            obj = {"fields": row_dict}
            existing_objs.append(obj)
            name_index[key] = len(existing_objs) - 1

        # Build mappings snapshot from store first
        obj["mappings"] = _snapshot_user_mappings_from_store(store, row_dict)

        # If mapping enabled, detect missing categories (no mappings found) and call LLM once to enrich store
        if enable_mapping:
            missing = any(len(obj["mappings"].get(cat, [])) == 0 and _norm_key(row_dict.get(cat, "")) for cat in ("Role","Expertise","Interest"))
            if missing:
                llm_out = get_mappings(row_dict.get("Role"), row_dict.get("Expertise"), row_dict.get("Interest"), llm_cfg)
                if llm_out:
                    # Update the shared store (merge; never overwrite)
                    _update_store_with_llm(store, llm_out)
                    # Refresh the snapshot for this user from the (now enriched) store
                    obj["mappings"] = _snapshot_user_mappings_from_store(store, row_dict)

    # Write pretty JSON for users
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(existing_objs, f, ensure_ascii=False, indent=2)

    # Persist the shared mappings store
    _save_store(store_path, store)

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

    # Outputs
    append_csv(values, args.csv_out)
    write_json_pretty(
        values,
        args.json_out,
        args.mappings_store,
        llm_cfg,
        args.enable_llm_mapping
    )

if __name__ == "__main__":
    main()
