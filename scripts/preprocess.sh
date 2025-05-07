#!/bin/bash

echo "Setting up python"
bash scripts/setup_python.sh

echo "Data Collection"
bash scripts/data_collection.sh
echo "Data Collection Complete!"