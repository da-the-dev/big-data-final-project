#!/bin/bash

# Run the Python script to build the database
echo "Building PostgreSQL database..."

source .venv/bin/activate
python scripts/build_projectdb.py

echo "Data storage completed successfully!"