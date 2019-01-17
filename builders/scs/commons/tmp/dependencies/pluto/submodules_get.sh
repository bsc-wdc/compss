#!/bin/sh

set -e

git submodule sync
git submodule init
git submodule update
