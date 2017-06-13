#!/bin/bash -e

  # Installation
  #./install_deps.sh
  ./install_compss.sh

  # Sync data
  svnUser=cramonco
  ./sync_with_svn.sh $svnUser

  # Test tutorial apps
  # TODO: Currently this step is done manually

  # Clean
  ./clean_ova.sh
  ./clean_history.sh

