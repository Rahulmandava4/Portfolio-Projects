#!/usr/bin/env python3
"""
extract_chainlink.py

Fetch the full tag, release, and commit history (all time) for the
`smartcontractkit/chainlink` GitHub repository, then validate the
fetched data against JSON schemas, and save results to JSON and CSV:

- JSON:  chainlink_version_history.json
- CSV:   chainlink_commits.csv

Usage:
    python extract_chainlink.py

Ensure GITHUB_TOKEN is set in the environment.
"""
import os
import sys
import requests
import json
import csv
from datetime import datetime
from jsonschema import validate, ValidationError

# --- Configuration ---
OWNER = "smartcontractkit"
REPO  = "chainlink"
TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    raise RuntimeError("Please set the GITHUB_TOKEN environment variable.")
HEADERS  = {"Authorization": f"token {TOKEN}"}
BASE_URL = f"https://api.github.com/repos/{OWNER}/{REPO}"

# --- JSON Schemas ---
def get_tag_schema():
    return {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "sha":  {"type": "string"}
        },
        "required": ["name", "sha"]
    }

def get_release_schema():
    return {
        "type": "object",
        "properties": {
            "tag_name":     {"type": "string"},
            "published_at": {"type": "string", "format": "date-time"},
            "body":         {"type": "string"}
        },
        "required": ["tag_name", "published_at", "body"]
    }

def get_commit_schema():
    return {
        "type": "object",
        "properties": {
            "sha":     {"type": "string"},
            "message": {"type": "string"},
            "author":  {"type": ["string", "null"]},
            "date":    {"type": "string", "format": "date-time"}
        },
        "required": ["sha", "message", "author", "date"]
    }

# --- Fetch functions ---
def fetch_tags():
    tags, page = [], 1
    while True:
        resp = requests.get(f"{BASE_URL}/tags", headers=HEADERS,
                             params={"per_page": 100, "page": page})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for t in batch:
            tags.append({"name": t["name"], "sha": t["commit"]["sha"]})
        page += 1
    return tags


def fetch_releases():
    releases, page = [], 1
    while True:
        resp = requests.get(f"{BASE_URL}/releases", headers=HEADERS,
                             params={"per_page": 100, "page": page})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for r in batch:
            releases.append({
                "tag_name":     r.get("tag_name"),
                "published_at": r.get("published_at"),
                "body":         r.get("body") or ""
            })
        page += 1
    return releases


def fetch_all_commits():
    commits, page = [], 1
    while True:
        resp = requests.get(f"{BASE_URL}/commits", headers=HEADERS,
                             params={"per_page": 100, "page": page})
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            break
        for c in batch:
            cm = c.get("commit", {})
            commits.append({
                "sha":     c.get("sha"),
                "message": cm.get("message", "").split("\n", 1)[0],
                "author":  cm.get("author", {}).get("name"),
                "date":    cm.get("author", {}).get("date")
            })
        page += 1
    return commits

# --- Validation functions ---
def validate_items(items, schema_fn, item_type):
    schema = schema_fn()
    errors = []
    for idx, item in enumerate(items, start=1):
        try:
            validate(instance=item, schema=schema)
        except ValidationError as e:
            identifier = item.get('name') or item.get('tag_name') or item.get('sha')
            errors.append((item_type, idx, identifier, e.message))
    return errors

# --- Save functions ---
def save_json(data, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2)
    print(f"✔ JSON saved to {filename}")


def save_commits_csv(commits, filename):
    if not commits:
        print("No commits to write to CSV.")
        return
    fieldnames = ["sha", "message", "author", "date"]
    with open(filename, 'w', newline='', encoding='utf-8') as csvf:
        writer = csv.DictWriter(csvf, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(commits)
    print(f"✔ CSV saved to {filename}")

# --- Main flow ---
def main():
    # Fetch
    print(f"Fetching tags for {OWNER}/{REPO}...")
    tags = fetch_tags()
    print(f"Retrieved {len(tags)} tags.")

    print(f"Fetching releases for {OWNER}/{REPO}...")
    releases = fetch_releases()
    print(f"Retrieved {len(releases)} releases.")

    print("Fetching all commits (this may take some time)...")
    commits = fetch_all_commits()
    print(f"Retrieved {len(commits)} commits.")

    # Validate
    print("Validating tags...")
    tag_errors = validate_items(tags, get_tag_schema, 'Tag')
    print("Validating releases...")
    rel_errors = validate_items(releases, get_release_schema, 'Release')
    print("Validating commits...")
    commit_errors = validate_items(commits, get_commit_schema, 'Commit')

    all_errors = tag_errors + rel_errors + commit_errors
    if all_errors:
        print(f"\nFound {len(all_errors)} validation errors:")
        for typ, idx, ident, msg in all_errors:
            print(f"  [{typ} #{idx} / {ident}]: {msg}")
        sys.exit(1)
    print("All data passed validation.")

    # Save
    output = {
        "repository":   f"{OWNER}/{REPO}",
        "retrieved_at": datetime.utcnow().isoformat() + "Z",
        "tags":         tags,
        "releases":     releases,
        "commits":      commits
    }
    json_file = f"{REPO}_version_history.json"
    csv_file  = f"{REPO}_commits.csv"
    save_json(output, json_file)
    save_commits_csv(commits, csv_file)

if __name__ == '__main__':
    main()
