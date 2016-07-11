#!/bin/bash -e

  wget -qO - http://compss.bsc.es/repo/debs/deb-gpg-bsc-grid.pub.key | apt-key add -
  wget http://compss.bsc.es/releases/repofiles/repo_deb_ubuntu_x86-64_unstable.list -O /etc/apt/sources.list.d/compss-framework_x86-64.list
  apt-get update
  apt-get -y --force-Yes install compss-framework

  # Exit with status from last command
  exit 

