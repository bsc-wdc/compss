#!/bin/sh -e
  #Get parameter
  type=$1

  echo "Erasing previous installations";
  zypper remove --non-interactive -y --clean-deps compss-framework
  zypper removerepo -f compss

  echo "Installing new version"
  zypper addrepo -f http://compss.bsc.es/repo/rpms/testing/suse/${type} compss
  zypper --non-interactive --no-gpg-checks refresh
  zypper install -y compss-framework

  # Exit with install cmd status
  exit 

