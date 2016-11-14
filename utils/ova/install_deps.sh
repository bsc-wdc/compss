#/bin/bash -e

  # Update apt
  sudo apt-get update

  # Developer deps
  sudo apt-get -y --force-Yes install subversion maven

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

