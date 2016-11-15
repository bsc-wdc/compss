#!/bin/bash -e

  # Installation
  ./install_deps.sh
  ./install_compss.sh

  # Sync data
  ./sync_with_svn.sh

  # Test tutorial apps
  # TODO: Currently this step is done manually

  # Clean
  ./clean_ova.sh

  # TODO: Check if clean history removes all the current bash_history
  ./clean_history.sh

