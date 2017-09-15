#!/bin/bash -e

  cleanRepo() {
    rm -f /etc/apt/sources.list.d/compss-framework_x86-64.list
  }

  # Trap for clean
  trap cleanRepo EXIT

  # Download repository and keys
  wget -qO - http://compss.bsc.es/repo/debs/deb-gpg-bsc-grid.pub.key | sudo apt-key add -
  wget http://compss.bsc.es/releases/repofiles/repo_deb_debian_noarch_unstable.list -O /etc/apt/sources.list.d/compss-framework_noarch.list
  
  # Refresh and install
  apt-get update
  apt-get -y --force-Yes install compss-framework

  # Exit with status from last command
  exit 

