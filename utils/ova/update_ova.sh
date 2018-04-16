#!/bin/bash -e

  # Installation
  #./install_deps.sh
  ./install_compss.sh

  # Sync data
  gitUser=jenkins
  ./sync_with_git.sh $gitUser

  # Test tutorial apps
  # TODO: Currently this step is done manually

  # Clean
  ./clean_ova.sh
  ./clean_history.sh

