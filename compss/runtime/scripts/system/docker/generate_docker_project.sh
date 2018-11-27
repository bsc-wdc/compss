#!/bin/bash

  # Retrieve script arguments
  project_file=$1
  num_workers=$2
  image_name=$3
  min_vms=$4
  max_vms=$5

  # Define script constants
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # shellcheck source=../xmls/generate_project.sh
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}"/../xmls/generate_project.sh

  # Init project file
  init "${project_file}"
  # Add header (from generate_project.sh)
  add_header
  # Add master information (from generate_project.sh)
  add_master_node ""
  # Add workers (from generate_project.sh)
  for (( i=1; i<=num_workers; i++ )); do
    add_compute_node "worker$i" "/opt/COMPSs/" "/tmp/worker$i" "" "" "" "" "" ""
  done
  # Add cloud
  props="vm-user=root vm-keypair-name=id_rsa vm-keypair-location=~/.ssh/"
  its="small medium large extra_large"
  add_cloud 0 "$min_vms" "$max_vms" "Docker" "${props}" "$image_name" "/opt/COMPSs/" "/root/" "root" "" "" "" "" "" "${its}"
  # Close project (from generate_project.sh)
  add_footer

