#!/bin/bash -e

  usage() {
    local exitValue=$1

    echo " "
    echo "Usage:"
    echo "  reinstall.sh                                   : For default COMPSs installation"
    echo "  reinstall.sh <COMPSs_installation_base_dir>    : For custom COMPSs installation"
    echo " "
   
    exit $exitValue
  }
  

  #---------------------------------------------------------------------
  # MAIN PROGRAM
  #---------------------------------------------------------------------

  #Check parameters
  if [[ $# -ne 0  && $# -ne 1 ]]; then
    echo "ERROR: Illegal number of parameters."
    usage 1
  fi
  if [[ $# -eq 1 ]]; then
    if [ "$1" == "usage" ]; then
      usage 0
    fi
  fi

  #Get parameters if available
  if [ "$#" -eq 1 ]; then
    COMPSs_install_dir=$1
    echo "* Custom COMPSs installation dir provided:"
    echo "        ${COMPSs_install_dir}"
  else
    COMPSs_install_dir=/opt/COMPSs/
    echo "* Default COMPSs installation dir provided:"
    echo "        ${COMPSs_install_dir}"
  fi

  #Define variables
  script_dir=$(dirname $0)

  echo "* Stop service"
  /etc/init.d/compss-monitor stop

  echo "* Erase previous deployment"
  sudo rm -f ${COMPSs_install_dir}/Tools/monitor/apache-tomcat/webapps/compss-monitor.war 
  sudo rm -rf ${COMPSs_install_dir}/Tools/monitor/apache-tomcat/webapps/compss-monitor
  
  echo "* Compile"
  cd ${script_dir}/../
  mvn -U clean install
  cd -

  echo "* Deploy"
  sudo cp ${script_dir}/../target/compss-monitor.war ${COMPSs_install_dir}/Tools/monitor/apache-tomcat/webapps/ 

  echo "* Start"
  /etc/init.d/compss-monitor start

  echo "* DONE"

