#!/bin/bash -e

  git submodule sync
  git submodule init
  git submodule update
  git submodule update --remote dependencies/dlb

