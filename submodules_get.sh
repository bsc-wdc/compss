#!/bin/bash -e

  git submodule sync
  git submodule init
  git submodule update
  # Git submodules must match git clones from Dockerfile
  git submodule update --remote compss/programming_model/bindings/RCOMPSs
  git submodule update --remote dependencies/threadpoolctl