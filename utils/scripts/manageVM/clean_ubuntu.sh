#!/bin/bash -e

  #---------------------------------------------------------------------------------------------------------------------
  #Unninstall needed software
  echo "--- Unninstalling needed software..."
  sudo apt-get -y --force-Yes remove compss-framework

  #Clean
  sudo apt-get -y --force-Yes autoremove
  sudo rm -f /etc/apt/sources.list.d/compss-framework_x86-64.list

  sudo apt-get clean
  sudo apt-get update

  echo "      Success"
  
