#!/bin/bash -e

  #Define script variables
  vm_user=
  COMPSs_version=

  #---------------------------------------------------------------------------------------------------------------------
  #Install needed software
  echo "--- Installing needed software..."
  sudo apt-get update

  # Build dependencies
  sudo apt-get -y --force-Yes install maven subversion

  # Runtime dependencies
  sudo apt-get -y --force-Yes install openjdk-8-jdk graphviz xdg-utils
  # Bindings-common-dependencies
  sudo apt-get -y --force-Yes install libtool automake build-essential
  # Python-binding dependencies
  sudo apt-get -y --force-Yes install python-dev
  # C-binding dependencies
  sudo apt-get -y --force-Yes install libxml2-dev libboost-serialization-dev libboost-iostreams-dev csh
  # Extrae dependencies
  sudo apt-get -y --force-Yes install libxml2 gfortran

  export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/
  echo "      Success"
 

  #---------------------------------------------------------------------------------------------------------------------
  #Download COMPSs repository
  echo "--- Unpackaging COMPSs SVN Revision..."
  cd /home/${vm_user}/
  tar -xzf compss.tar.gz


  #---------------------------------------------------------------------------------------------------------------------
  #Compile, build and package COMPSs
  echo "--- Compile, build and package COMPSs..."
  cd /home/${vm_user}/tmpTrunk/builders/specs/deb
  ./builddeb "ubuntu" ${COMPSs_version}

