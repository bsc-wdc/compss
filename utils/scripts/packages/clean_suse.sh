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
  sudo zypper --non-interactive remove -y --clean-deps maven
  sudo zypper --non-interactive remove -y --clean-deps rpm-build
  #Runtime dependencies
  sudo zypper --non-interactive remove -y --clean-deps java-1_8_0-openjdk java-1_8_0-openjdk-devel graphviz xdg-utils
  #Bindings-common-dependencies
  sudo zypper --non-interactive remove -y --clean-deps java-devel libtool automake make gcc-c++
  #Python-binding dependencies
  sudo zypper --non-interactive remove -y --clean-deps python-devel
  #C-binding dependencies
  sudo zypper --non-interactive remove -y --clean-deps libxml2-devel boost-devel tcsh
  #Extrae dependencies
  sudo zypper --non-interactive remove -y --clean-deps libxml2 gcc-fortran papi-devel papi
  #Clean
  sudo zypper rr mvn
  sudo zypper refresh

  echo "      Success"

