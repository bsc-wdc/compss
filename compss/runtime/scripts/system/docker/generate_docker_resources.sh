#!/bin/bash

# Setting up COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  COMPSS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../.. && pwd )/"
fi
if [ ! "${COMPSS_HOME: -1}" = "/" ]; then
  COMPSS_HOME="${COMPSS_HOME}/"
fi
export COMPSS_HOME=${COMPSS_HOME}


  # Retrieve script arguments
  resources_file=$1
  num_workers=$2
  cus=$3
  memory=$4
  swarm_manager_ip=$5
  image_name=$6
  cloud=$7
  creation_time=$8

  # shellcheck source=../xmls/generate_resources.sh
  # shellcheck disable=SC1091
  source "${COMPSS_HOME}Runtime/scripts/system/xmls/generate_resources.sh"
  
  # Init resources file
  init "${resources_file}"
  # Add header (from generate_resources.sh)
  add_header
  # Add workers
  for (( i=1; i<=num_workers; i++ )); do
    add_compute_node "worker$i" "$cus" "0" "0" "$memory" "$((40000 + 2*(i-1) + 1))" "$((40000 + 2*(i-1) + 2))" "" ""
  done
  if [ "${cloud}" == "True" ]; then
    echo "Adding cloud ..."
    its="small:1:0:0:2.0:1:0.85 medium:2:0:0:3.0:1:0.95 large:4:0:0:4.0:1:1.25 extra_large:8:0:0:8.0:1:2.25"
    add_cloud "Docker" "tcp://$swarm_manager_ip" "docker-conn.jar" "es.bsc.conn.docker.Docker" "$image_name" "" "$creation_time" "43000" "43003" "" "${its}"
  fi
  # Close resources (from generate_resources.sh)
  add_footer

