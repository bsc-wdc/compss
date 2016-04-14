#!/bin/bash -e

  #---------------------------------------------------------------------------------------------------------------------
  #Unninstall needed software
  echo "--- Unninstalling needed software..."
  sudo zypper --non-interactive remove -y --clean-deps compss-framework

  #Clean
  sudo zypper rr mvn
  sudo zypper rr compss
  sudo zypper refresh

  echo "      Success"

