#!/bin/bash

  ROOT_FOLDER=/
  SHAREDDISK=/sharedDisk/
 
  # Add 0's to HOME
  echo "Adding 0's to $HOME"
  cd $HOME
  dd if=/dev/zero of=zeroFile bs=1M
  rm -f zeroFile

  # Add 0's to sharedDisk
  echo "Adding 0's to $SHAREDDISK"
  cd $SHAREDDISK
  dd if=/dev/zero of=zeroFile bs=1M
  rm -f zeroFile

  # Add 0's to HOME
  echo "Adding 0's to $ROOT_FOLDER"
  cd $ROOT_FOLDER
  sudo dd if=/dev/zero of=zeroFile bs=1M
  sudo rm -f zeroFile

  # End
  echo "DONE!"
  exit 0

