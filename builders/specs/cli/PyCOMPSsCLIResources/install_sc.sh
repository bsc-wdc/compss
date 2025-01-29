#!/usr/bin/env bash

./build.sh

echo "Installing..."

version=$(python -c "import sys; version = sys.version_info; print('%d.%d' % (version[0], version[1]))")
path="$1/lib/python${version}/site-packages/"

mkdir -p ${path}

export PYTHONPATH=${path}:$PYTHONPATH
python3 -m pip install . --target $1 --no-deps

echo "----- Installation finished -----"
