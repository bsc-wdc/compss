#!/bin/bash -e

  #---------------------------------------------------------------------------------------------------------------------
  # Unninstall needed software
  echo "--- Unninstalling needed software..."
  sudo yum remove -y compss-framework
  sudo rm -f /etc/yum.repos.d/compss-framework_x86_64.repo

  # Remove unnoffical mvn
  sudo rm -rf /usr/local/apache-maven-3.3.3
  sudo rm -rf /usr/local/maven
  sudo rm -f /etc/profile.d/maven.sh

  # Clean
  sudo yum clean all
  sudo yum update -y

  echo "      Success"

