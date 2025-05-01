#!/bin/bash

echo "Step 1: PostgreSQL"
bash scripts/data_storage.sh

echo "Step 2: Sqoop"
bash scripts/data_ingestion.sh

echo "Stage 1 completed successfully!"