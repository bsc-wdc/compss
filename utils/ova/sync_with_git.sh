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
  #sudo rm -rf trunk
  sudo rm -rf tutorial_apps traces
  #svn co http://compss.bsc.es/svn/compss/framework/trunk trunk --username $svnUser
  # TODO: put here new apps repo svn co http://compss.bsc.es/svn/bar/tutorial_apps/ tutorial_apps
  # --username $svnUser
  # TODO: put here new traces rep svn co http://compss.bsc.es/svn/bar/traces traces --username
  # $svnUser
  
  # Clean unneeded files
  rm -rf tutorial_apps/ide/

  # Check out datasets
  cd $SHAREDDISK
  rm -rf $SHAREDDISK*
  # TODO: put here new datasets rep svn co http://compss.bsc.es/svn/bar/datasets . --username
  # $svnUser

  # Clean unneeded files
  rm -rf Discrete/ GeneDetection/ Hmmer/ nmmb/

  cd -

  # Retrieve status
  echo "APPS, TRACES, and DATASETS NOT ADDED, need to configure GIT repos. DONE!"
  exit 0

