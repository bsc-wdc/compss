#!/bin/bash -e

  cleanRepo() {
    zypper removerepo compss
  }

  # Trap for clean
  trap cleanRepo EXIT

  # Download repository and keys
  zypper addrepo -f http://compss.bsc.es/repo/rpms/unstable/suse/x86_64 compss
  
  # Refresh and install
  zypper --non-interactive --no-gpg-checks refresh
  zypper install -y compss-framework

  # Exit with status from last command
  exit 

