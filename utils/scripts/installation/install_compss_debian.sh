#!/bin/bash -e

  cleanRepo() {
    rm -f /etc/apt/sources.list.d/compss-framework_x86-64.list

    if [ "${pack_folder}" != "" ]; then
      rm -rf ${pack_folder}
    fi
  }

  # Trap for clean
  trap cleanRepo EXIT

  # Download repository and keys
  wget -qO - http://compss.bsc.es/repo/debs/deb-gpg-bsc-grid.pub.key | sudo apt-key add -
  wget http://compss.bsc.es/releases/repofiles/repo_deb_debian_noarch_unstable.list -O /etc/apt/sources.list.d/compss-framework_noarch.list
  
  # Refresh and install
  #apt-get update
  #apt-get -y --force-Yes install compss-framework

  # Java8 is not supported officially in our debian distribution so we break the packages to perform the installation
  pack_folder=$(mktemp -d)
  cd ${pack_folder}
  apt-get download compss-*
  apt-get -y --force-Yes install libboost-serialization-dev libboost-iostreams-dev
  dpkg -i --force-all compss-*
  cd -

  # Exit with status from last command
  exit 

