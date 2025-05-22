from jsonschema import validate, ValidationError

release_schema = {
    "type": "object",
    "properties": {
        "tag_name": {"type": "string"},
        "published_at": {"type": "string", "format": "date-time"},
        "body": {"type": "string"}
    },
    "required": ["tag_name", "published_at", "body"]
}

# Validate each release:
for release in releases:
    try:
        validate(instance=release, schema=release_schema)
    except ValidationError as e:
        print(f"Release validation error: {e.message}")
