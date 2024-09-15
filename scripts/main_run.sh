#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$SCRIPT_DIR"

python "$BASE_DIR/download_multiprocess.py"
python "$BASE_DIR/run_fundamentals.py"
python "$BASE_DIR/daily_crossover.py"
python "$BASE_DIR/crossover_trading_res.py"
