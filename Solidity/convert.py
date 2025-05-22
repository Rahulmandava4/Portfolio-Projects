import json
import pandas as pd

# Load JSON data
with open("structured_version_history.json", "r") as f:
    data = json.load(f)

# Flatten the data: create a list of rows, one per commit, with release info
rows = []
for release in data['releases']:
    tag = release['tag']
    published_at = release['published_at']
    for commit in release.get("commits", []):
        rows.append({
            "Release Tag": tag,
            "Published At": published_at,
            "Commit SHA": commit["sha"],
            "Commit Message": commit["message"],
            "Author": commit["author"],
            "Commit Date": commit["date"]
        })

# Create a DataFrame and save as CSV
df = pd.DataFrame(rows)
df.to_csv("version_history.csv", index=False)

print("CSV file version_history.csv has been created.")
