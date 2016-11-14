#!/bin/bash -e

  ROOT_FOLDER=/
  SHAREDDISK=/sharedDisk/
 
  # Clean apt
  echo "Cleaning APT-GET"
  sudo apt-get -y --force-Yes update
  sudo apt-get -y --force-Yes autoremove
  sudo apt-get -y --force-Yes autoclean
  sudo apt-get -y --force-Yes purge
  sudo apt-get -y --force-Yes clean

  # Clean subversion
  echo "Cleanning SVN/MAVEN/COMPSs files"
  cd $ROOT_FOLDER
  sudo find . -name ".svn" | xargs -r -i -t rm -rf {}
  sudo find . -name ".subversion" | xargs -r -i -t rm -rf {}
  sudo find . -name ".m2" | xargs -r -i -t rm -rf {}
  sudo find . -name ".COMPSs" | xargs -r -i -t rm -rf {}
 
  echo "Adding 0's"
  # Add 0's to HOME
  cd $HOME
  dd if=/dev/zero of=zeroFile bs=1M
  rm -f zeroFile

  # Add 0's to sharedDisk
  cd $SHAREDDISK
  dd if=/dev/zero of=zeroFile bs=1M
  rm -f zeroFile

  # Add 0's to HOME
  cd $ROOT_FOLDER
  sudo dd if=/dev/zero of=zeroFile bs=1M
  sudo rm -f zeroFile

  # End
  echo "DONE!"
  exit 0

