#!/usr/bin/env bash

echo "Cleaning..."

rm -rf -v build/
rm -rf -v dist/
rm -rf -v pycompss_player.egg-info/
rm -rf __pycache__
rm -f -v *.pyc *.pyo

echo "----- Cleaning finished -----"
