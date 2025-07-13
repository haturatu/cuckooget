#!/bin/bash

set -e

command -v pyenv || "Please install pyenv"  
command -v python3 || "Prease install Python3"

pip install . 
cd lib
maturin build
pip install target/wheels/*.whl
cd -
rm -rf build lib/target src/cuckooget.egg-info cuckooget.egg-info

echo "Done! How to use `ck -h`"
