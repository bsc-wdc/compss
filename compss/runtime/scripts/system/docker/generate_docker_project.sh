#!/bin/bash

  # Setting up COMPSs_HOME
  if [ -z "${COMPSS_HOME}" ]; then
    COMPSS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../../.. && pwd )/"
  fi
  if [ ! "${COMPSS_HOME: -1}" = "/" ]; then
    COMPSS_HOME="${COMPSS_HOME}/"
  fi
  export COMPSS_HOME=${COMPSS_HOME}

  # Retrieve script arguments
  project_file=$1
  num_workers=$2
  image_name=$3
  min_vms=$4
  max_vms=$5

  # shellcheck source=../xmls/generate_project.sh
  # shellcheck disable=SC1091
  source "${COMPSS_HOME}Runtime/scripts/system/xmls/generate_project.sh"

  # Init project file
  init "${project_file}"
  #Add header (from generate_project.sh)
  add_header
  #Add master information (from generate_project.sh)
  add_master_node ""
  #Add workers (from generate_project.sh)
  for (( i=1; i<=num_workers; i++ )); do
    add_compute_node "worker$i" "/opt/COMPSs/" "/tmp/worker$i" "" "" "" "" "" ""
  done
  if [ ${max_vms} -gt 0 ]; then
    echo "Setting a cloud provider with min_vms: ${min_vms} and max_vms: ${max_vms}"
    props="vm-user=root vm-keypair-name=id_rsa vm-keypair-location=~/.ssh/"
    its="small medium large extra_large"
    add_cloud 0 "$min_vms" "$max_vms" "Docker" "${props}" "$image_name" "/opt/COMPSs/" "/root/" "root" "" "" "" "" "" "${its}"
  fi
  # Close project (from generate_project.sh)
  add_footer

