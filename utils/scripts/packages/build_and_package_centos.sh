#!/bin/bash -e
  
  #Define script variables
  vm_user=
  COMPSs_version=

  #---------------------------------------------------------------------------------------------------------------------
  #Install needed software
  echo "--- Installing needed software..."
  # Runtime dependencies
  sudo yum install -y java-1.8.0-openjdk java-1.8.0-openjdk-devel graphviz xdg-utils
  # Bindings-common-dependencies
  sudo yum install -y java-devel libtool automake make gcc-c++
  # Python-binding dependencies
  sudo yum install -y python-devel
  # C-binding dependencies
  sudo yum install -y libxml2-devel boost-devel tcsh
  # Extrae dependencies
  sudo yum install -y libxml2 gcc-gfortran papi papi-devel

  # Build dependencies
  sudo yum install -y rpm-build subversion
  cd /usr/local/
  sudo wget http://compss.bsc.es/repo/apache-maven/apache-maven-3.3.9-bin.tar.gz
  sudo tar -zxf apache-maven-3.3.9-bin.tar.gz
  sudo rm apache-maven-3.3.9-bin.tar.gz
  sudo ln -s apache-maven-3.3.9 maven
  cd -
  cat >> maven.sh << EOT
export M2_HOME=/usr/local/maven/
export PATH=\$PATH:\${M2_HOME}/bin/
EOT
  sudo mv maven.sh /etc/profile.d/maven.sh
  
  # Export variables for build
  source /etc/profile.d/maven.sh
  export JAVA_HOME=/usr/lib/jvm/java-openjdk/
  echo "      Success"

  
  #---------------------------------------------------------------------------------------------------------------------
  # Download COMPSs repository
  echo "--- Unpackaging COMPSs SVN Revision..."
  cd /home/${vm_user}/
  tar -xzf compss.tar.gz


  #---------------------------------------------------------------------------------------------------------------------
  # Compile, build and package COMPSs
  echo "--- Compile, build and package COMPSs..."
  cd /home/${vm_user}/tmpTrunk/builders/specs/rpm
  ./buildrpm "centos" ${COMPSs_version}
 
