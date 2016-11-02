#!/bin/bash -e

  #Define script variables
  vm_user=
  COMPSs_version=

  #---------------------------------------------------------------------------------------------------------------------
  #Install needed software
  echo "--- Installing needed software..."
  sudo zypper ar http://download.opensuse.org/repositories/Application:/Geo/openSUSE_13.1/ mvn
  sudo zypper --gpg-auto-import-keys refresh

  # Build dependencies
  sudo zypper install -y maven rpm-build subversion

  # Runtime dependencies
  sudo zypper install -y java-1_8_0-openjdk java-1_8_0-openjdk-devel graphviz xdg-utils
  # Bindings-common-dependencies
  sudo zypper install -y libtool automake make gcc-c++
  # Python-binding dependencies
  sudo zypper install -y python-devel
  # C-binding dependencies
  sudo zypper install -y libxml2-devel boost-devel tcsh
  # Extrae dependencies
  sudo zypper install -y libxml2 gcc-fortran papi papi-devel

  export JAVA_HOME=/usr/lib64/jvm/java-1.8.0-openjdk-1.8.0/
  echo "      Success"

  
  #---------------------------------------------------------------------------------------------------------------------
  #Download COMPSs repository
  echo "--- Unpackaging COMPSs SVN Revision..."
  cd /home/${vm_user}/
  tar -xzf compss.tar.gz


  #---------------------------------------------------------------------------------------------------------------------
  #Compile, build and package COMPSs
  echo "--- Compile, build and package COMPSs..."
  cd /home/${vm_user}/tmpTrunk/builders/specs/rpm
  ./buildrpm "suse" ${COMPSs_version}
 
