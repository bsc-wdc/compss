#!/bin/bash -e

  cleanRepo() {
    rm -f /etc/yum.repos.d/compss-framework_x86_64.repo
  }

  # Trap for clean
  trap cleanRepo EXIT

  # Download repository and keys
  wget http://compss.bsc.es/releases/repofiles/repo_rpm_centos_x86-64_unstable.repo -O /etc/yum.repos.d/compss-framework_x86_64.repo

  # Refresh and install
  yum --enablerepo=compss clean all
  yum update -y
  yum install -y compss-framework

  # Exit with status from last command
  exit

