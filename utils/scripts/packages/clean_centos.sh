#!/bin/bash -e
  #Define script variables
  vm_user=

  #---------------------------------------------------------------------------------------------------------------------
  # Clean created folders
  echo "--- Cleaning created folders"
  sudo rm -rf /home/${vm_user}/tmpTrunk
  echo "      Success"


  #---------------------------------------------------------------------------------------------------------------------
  # Unninstall needed software
  echo "--- Unninstalling needed software..."
  sudo yum remove -y rpm-build

  # We do not remove maven because it hasn't an official repo and can be potentially usefull
  #sudo rm /etc/profile.d/maven.sh
  #sudo rm /usr/local/maven
  #sudo rm -r /usr/local/apache-maven-3.3.3 

  # Runtime dependencies
  sudo yum remove -y java-1.8.0-openjdk java-1.8.0-openjdk-devel graphviz xdg-utils
  # Bindings-common-dependencies
    #None
  # Python-binding dependencies
  sudo yum remove -y python-devel
  # C-binding dependencies
  sudo yum remove -y libxml2-devel boost-devel tcsh
  # Extrae dependencies
  sudo yum remove -y gcc-gfortran papi-devel papi
  # Clean
  sudo yum clean all

  echo "      Success"

