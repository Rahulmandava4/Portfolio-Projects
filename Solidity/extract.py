import os
import requests
import json
from datetime import datetime

print("DEBUG GITHUB_TOKEN:", os.getenv("GITHUB_TOKEN"))

# Repository details
OWNER = "ethereum"
REPO = "solidity"

# Retrieve token from environment variable
TOKEN = os.getenv("GITHUB_TOKEN")
if not TOKEN:
    raise Exception("Please set the GITHUB_TOKEN environment variable.")

# Headers for API requests
HEADERS = {"Authorization": f"token {TOKEN}"}

# Base GitHub API URL for the repository
BASE_URL = f"https://api.github.com/repos/{OWNER}/{REPO}"
def fetch_tags():
    tags_url = f"{BASE_URL}/tags"
    response = requests.get(tags_url, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Error fetching tags: {response.status_code}")
    tags = response.json()
    # Return a list of tag dictionaries with name and commit SHA
    return [{"name": tag["name"], "sha": tag["commit"]["sha"]} for tag in tags]

# Test fetching tags
tags = fetch_tags()
print("Fetched Tags:")
for tag in tags:
    print(f"Tag: {tag['name']} - SHA: {tag['sha']}")
def fetch_releases():
    releases_url = f"{BASE_URL}/releases"
    response = requests.get(releases_url, headers=HEADERS)
    if response.status_code != 200:
        raise Exception(f"Error fetching releases: {response.status_code}")
    releases = response.json()
    # Return a list of release dictionaries with tag, date, and body (notes)
    return [{
        "tag_name": release.get("tag_name"),
        "published_at": release.get("published_at"),
        "body": release.get("body")
    } for release in releases]

# Test fetching releases
releases = fetch_releases()
print("\nFetched Releases:")
for release in releases:
    print(f"Release: {release['tag_name']} - Published at: {release['published_at']}")
def fetch_commits(since_date, until_date):
    commits_url = f"{BASE_URL}/commits"
    params = {"since": since_date, "until": until_date}
    response = requests.get(commits_url, headers=HEADERS, params=params)
    if response.status_code != 200:
        raise Exception(f"Error fetching commits: {response.status_code}")
    commits = response.json()
    # Return a list of commits with SHA, message, author name, and commit date
    commit_list = []
    for commit in commits:
        commit_data = commit.get("commit", {})
        commit_list.append({
            "sha": commit.get("sha"),
            "message": commit_data.get("message").split("\n")[0],  # first line of message
            "author": commit_data.get("author", {}).get("name"),
            "date": commit_data.get("author", {}).get("date")
        })
    return commit_list

# Example dates (update these to actual tag dates from your dataset)
SINCE_DATE = "2024-01-01T00:00:00Z"
UNTIL_DATE = "2025-03-22T00:00:00Z"

commits = fetch_commits(SINCE_DATE, UNTIL_DATE)
print("\nFetched Commits:")
for c in commits:
    print(f"{c['sha'][:7]}: {c['message']} (by {c['author']} on {c['date']})")
def save_data_to_file(tags, releases, commit_data, filename="version_history.json"):
    data = {
        "repository": f"{OWNER}/{REPO}",
        "retrieved_at": datetime.utcnow().isoformat() + "Z",
        "tags": tags,
        "releases": releases,
        "commits_between_sample_dates": commit_data  # For demonstration; you can structure this per release
    }
    with open(filename, "w") as f:
        json.dump(data, f, indent=2)
    print(f"\nData saved to {filename}")

# Save all the data to a JSON file
save_data_to_file(tags, releases, commits)
