#!/bin/bash -e

  # Installation
  ./install_deps.sh
  ./install_compss.sh

  # Sync data
  ./sync_with_svn.sh

  # Clean
  ./clean_ova.sh
  ./clean_history.sh

