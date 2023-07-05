#!/bin/bash

echo "Installing required dependencies to run the style..."

python3 -m pip install black "black[jupyter]"
python3 -m pip install pycodestyle
python3 -m pip install pydocstyle
python3 -m pip install flake8
