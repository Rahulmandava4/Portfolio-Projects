#!/bin/bash

# Runs the data scraper.
docker run -v "$(pwd)/scraper:/workspace" rust:1.86 bash -c "cd /workspace && cargo run --release -- ingest && cargo run --release -- postprocess-to-parquet"
