#!/bin/bash -e

  # Remove compss
  yum remove -y compss-*

  # Purge compss

  # Clean all
  wget http://compss.bsc.es/releases/repofiles/repo_rpm_centos_x86-64_unstable.repo -O /etc/yum.repos.d/compss-framework_x86_64.repo
  yum --enablerepo=compss clean all
  rm -f /etc/yum.repos.d/compss-framework_x86_64.repo

  # Update DB
  yum update -y

  # Exit with status from last command
  exit

