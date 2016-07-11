#!/bin/bash -e

  zypper addrepo -f http://compss.bsc.es/repo/rpms/unstable/suse/x86_64 compss
  zypper --non-interactive --no-gpg-checks refresh
  zypper install -y compss-framework

  # Exit with status from last command
  exit 

