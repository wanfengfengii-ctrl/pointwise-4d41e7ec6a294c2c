#!/bin/sh
set -eu
cd "$(dirname "$0")"
python3.11 -B -m unittest discover -s tests -t . -v
exec python3.11 -B mixed_check.py verify
