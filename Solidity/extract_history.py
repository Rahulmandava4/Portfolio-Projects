import os
import requests
import json
from datetime import datetime
from jsonschema import validate, ValidationError

# --------------------------
# Configuration and Constants
# --------------------------
OWNER = "ethereum"
REPO = "solidity"

# Retrieve your GitHub token from the environment variable
TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    raise Exception("Please set the GITHUB_TOKEN environment variable.")

HEADERS = {"Authorization": f"token {TOKEN}"}
BASE_URL = f"https://api.github.com/repos/{OWNER}/{REPO}"

# --------------------------
# JSON Schemas for Validation
# --------------------------
tag_schema = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "sha": {"type": "string"}
    },
    "required": ["name", "sha"]
}

commit_schema = {
    "type": "object",
    "properties": {
        "sha": {"type": "string"},
        "message": {"type": "string"},
        "author": {"type": "string"},
        "date": {"type": "string", "format": "date-time"}
    },
    "required": ["sha", "message", "author", "date"]
}

release_schema = {
    "type": "object",
    "properties": {
        "tag_name": {"type": "string"},
        "published_at": {"type": "string", "format": "date-time"},
        "body": {"type": "string"}
    },
    "required": ["tag_name", "published_at", "body"]
}

# --------------------------
# Functions to Fetch and Validate Data
# --------------------------
def fetch_tags():
    url = f"{BASE_URL}/tags"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Error fetching tags: {response.status_code}")
    tags = response.json()
    validated_tags = []
    for tag in tags:
        tag_obj = {
            "name": tag.get("name"),
            "sha": tag.get("commit", {}).get("sha")
        }
        try:
            validate(instance=tag_obj, schema=tag_schema)
            validated_tags.append(tag_obj)
        except ValidationError as e:
            print(f"Tag validation error: {e.message}")
    return validated_tags

def fetch_releases():
    url = f"{BASE_URL}/releases"
    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Error fetching releases: {response.status_code}")
    releases = response.json()
    validated_releases = []
    for release in releases:
        release_obj = {
            "tag_name": release.get("tag_name"),
            "published_at": release.get("published_at"),
            "body": release.get("body", "")
        }
        try:
            validate(instance=release_obj, schema=release_schema)
            validated_releases.append(release_obj)
        except ValidationError as e:
            print(f"Release validation error: {e.message}")
    return validated_releases

def fetch_commits(since_date, until_date):
    url = f"{BASE_URL}/commits"
    params = {"since": since_date, "until": until_date}
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        raise Exception(f"Error fetching commits: {response.status_code}")
    commits = response.json()
    validated_commits = []
    for commit in commits:
        commit_data = commit.get("commit", {})
        commit_obj = {
            "sha": commit.get("sha"),
            "message": commit_data.get("message", "").split("\n")[0].strip(),
            "author": commit_data.get("author", {}).get("name", "Unknown"),
            "date": commit_data.get("author", {}).get("date")
        }
        try:
            validate(instance=commit_obj, schema=commit_schema)
            validated_commits.append(commit_obj)
        except ValidationError as e:
            print(f"Commit validation error for commit {commit_obj.get('sha')}: {e.message}")
    return validated_commits

# --------------------------
# Group Commits by Release
# --------------------------
def group_commits_by_release(releases):
    # Sort releases by published_at ascending (oldest first)
    releases_sorted = sorted(releases, key=lambda r: r["published_at"] if r["published_at"] else "")
    grouped = []
    previous_date = None
    for release in releases_sorted:
        current_date = release["published_at"]
        since_date = previous_date if previous_date else "2000-01-01T00:00:00Z"
        commits = fetch_commits(since_date, current_date)
        grouped.append({
            "tag": release["tag_name"],
            "published_at": current_date,
            "release_notes": release["body"],
            "commits": commits
        })
        previous_date = current_date
    return grouped

# --------------------------
# Main Process
# --------------------------
def main():
    print("Fetching tags...")
    tags = fetch_tags()
    print("Fetched and validated tags:")
    print(json.dumps(tags, indent=2))
    
    print("\nFetching releases...")
    releases = fetch_releases()
    print("Fetched and validated releases:")
    print(json.dumps(releases, indent=2))
    
    if releases:
        print("\nGrouping commits by release...")
        grouped_data = group_commits_by_release(releases)
    else:
        # If no releases, fetch commits for a sample interval
        grouped_data = [{
            "sample_interval_commits": fetch_commits("2024-01-01T00:00:00Z", "2025-03-22T00:00:00Z")
        }]
    
    final_data = {
        "repository": f"{OWNER}/{REPO}",
        "retrieved_at": datetime.utcnow().isoformat() + "Z",
        "tags": tags,
        "releases": grouped_data
    }
    
    with open("structured_version_history.json", "w") as f:
        json.dump(final_data, f, indent=2, default=str)
    
    print("\nStructured data saved to structured_version_history.json")

if __name__ == "__main__":
    main()
