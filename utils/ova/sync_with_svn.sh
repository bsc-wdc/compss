#/bin/bash -e

  usage() {
    echo "ERROR: Incorrect number of parameters"
    echo "Usage: $0 <svnUser>"
    echo " "
  }

  # Get parameters
  if [ $# -ne 1 ]; then
    usage
  fi
  svnUser=$1

  # Define script variables
  SHAREDDISK=/sharedDisk/

  # Check Out svns
  cd $HOME
  svn co http://compss.bsc.es/svn/compss/framework/trunk trunk --username $svnUser
  svn co http://compss.bsc.es/svn/bar/tutorial-apps/ tutorial-apps --username $svnUser
  svn co http://compss.bsc.es/svn/bar/traces traces --username $svnUser

  cd $SHAREDDISK
  svn co http://compss.bsc.es/svn/bar/datasets . --username $svnUser

  # Retrieve status
  echo "DONE!"
  exit 0

