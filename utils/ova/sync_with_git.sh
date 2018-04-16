#/bin/bash -e

  usage() {
    echo "ERROR: Incorrect number of parameters"
    echo "Usage: $0 <gitUser>"
    echo " "
  }

  # Get parameters
  if [ $# -ne 1 ]; then
    usage
  fi
  gitUser=$1

  # Define script variables
  SHAREDDISK=/sharedDisk/

  # Check Out git
  cd $HOME
  sudo rm -rf tutorial_apps traces
  git clone http://${gitUser}@compss.bsc.es/gitlab/bar/tutorial_apps.git


  # Clean unneeded files
  rm -rf tutorial_apps/ide/

  cd -

  # Retrieve status
  echo "DONE!"
  exit 0

