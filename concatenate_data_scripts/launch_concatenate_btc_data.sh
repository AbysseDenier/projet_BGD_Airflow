#!/bin/bash

PROJECT_ROOT="$1"

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
python3 "$SCRIPT_DIR/concatenate_btc_data.py" "$PROJECT_ROOT"
