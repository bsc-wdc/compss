#!/usr/bin/env bash

echo "Installing required `build` package..."

python3 -m pip install build

echo "Building..."

python3 -m build

echo "----- Building finished -----"
