#!/bin/bash

set -e

command -v python3 >/dev/null || { echo "Please install Python3"; exit 1; }
python3 -m pip show maturin >/dev/null 2>&1 || { echo "Please install maturin (python3 -m pip install maturin)"; exit 1; }

python3 -m pip install . --no-build-isolation --force-reinstall
rm -rf build lib/target src/cuckooget.egg-info cuckooget.egg-info

echo "Done! How to use `ck -h`"
