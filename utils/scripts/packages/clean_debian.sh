#!/bin/bash -e
  #Define script variables
  vm_user=

  #---------------------------------------------------------------------------------------------------------------------
  #Clean created folders
  echo "--- Cleaning created folders"
  sudo rm -rf /home/${vm_user}/tmpTrunk
  echo "      Success"


  #---------------------------------------------------------------------------------------------------------------------
  #Unninstall needed software
  echo "--- Unninstalling needed software..."
  sudo apt-get -y --force-Yes remove maven
  #Runtime dependencies
  sudo apt-get -y --force-Yes remove openjdk-8-jdk graphviz xdg-utils
  #Bindings-common-dependencies
  sudo apt-get -y --force-Yes remove libtool automake build-essential
  #Python-binding dependencies
  sudo apt-get -y --force-Yes remove python-dev
  #C-binding dependencies
  sudo apt-get -y --force-Yes remove libxml2-dev libboost-serialization-dev libboost-iostreams-dev csh
  #Extrae dependencies
  sudo apt-get -y --force-Yes remove libxml2 gfortran

  #Clean
  sudo apt-get -y --force-Yes autoremove
  sudo apt-get clean
  sudo apt-get update

  echo "      Success"
  
