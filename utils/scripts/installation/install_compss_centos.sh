#!/bin/bash -e

  wget http://compss.bsc.es/releases/repofiles/repo_rpm_centos_x86-64_unstable.repo -O /etc/yum.repos.d/compss-framework_x86_64.repo
  yum install -y compss-framework

  # Exit with status from last command
  exit

