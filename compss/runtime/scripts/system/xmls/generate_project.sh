#!/bin/bash

#
# SCRIPT CONSTANTS
#

DEFAULT_PROJECT_FILENAME="project.xml"

DEFAULT_NODE_NAME=""
DEFAULT_WORKER_INSTALL_DIR=""
DEFAULT_WORKER_WORKING_DIR=""
DEFAULT_USER=""
DEFAULT_APP_DIR=""
DEFAULT_LIBRARY_PATH=""
DEFAULT_CLASSPATH=""
DEFAULT_PYTHONPATH=""
DEFAULT_LIMIT_OF_TASKS=""

DEFAULT_INIT_VMS=0
DEFAULT_MIN_VMS=0
DEFAULT_MAX_VMS=""
DEFAULT_CLOUD_PROVIDER_NAME=""
DEFAULT_CLOUD_PROPERTIES=""
DEFAULT_IMAGE_NAME="None"
DEFAULT_INSTANCE_TYPES="default"


#
# INITIALIZATION
#

init() {
  PROJECT_FILE=${1:-$DEFAULT_PROJECT_FILENAME}
}


#
# PUBLIC FUNCTIONS
#

add_header() {
  cat > "${PROJECT_FILE}" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Project>
EOT
}

add_footer() {
  cat >> "${PROJECT_FILE}" << EOT
</Project>
EOT
}

add_master_node() {
  # Parameters are of the form sd_name=sd_mountpoint sd_name2=sd_mountpoint2

  # Retrieve parameters
  local cus=${1}
  local gpus=${2}
  local fpgas=${3}
  local memory=${4}
  shift 4
  local shared_disks="${*}"

  # Check parameters
  :
    cat >> "${PROJECT_FILE}" << EOT
  <MasterNode>
EOT


  # Dump information to file
   _fill_processors "${cus}" "${gpus}" "${fpgas}"
   _fill_memory "${memory}"

  if [ -n "${shared_disks}" ] && [ "${shared_disks}" != "NULL" ]; then
    # Create master node with shared disks
    cat >> "${PROJECT_FILE}" << EOT
    <SharedDisks>
EOT
    for sd in ${shared_disks}; do
      sd_name=${sd%=*}  # Deletes shortest =* from back
      sd_mountpoint=${sd##*=}  # Deletes longest *= from front
      cat >> "${PROJECT_FILE}" << EOT
      <AttachedDisk Name="${sd_name}">
        <MountPoint>${sd_mountpoint}</MountPoint>
      </AttachedDisk>
EOT
    done
    cat >> "${PROJECT_FILE}" << EOT
    </SharedDisks>
EOT
  fi

    cat >> "${PROJECT_FILE}" << EOT
  </MasterNode>

EOT

  # TODO: SKIPPING PRICE SECTION
}

add_compute_node() {
  # Retrieve parameters
  local node_name=${1:-$DEFAULT_NODE_NAME}
  shift 1
  local args_to_pass=$*

  # Check parameters
  if [ -z "${node_name}" ]; then
    echo "[ERROR] Cannot add compute node because node_name is empty"
    exit 1
  fi

  # Dump information to file

  # Open compute node
  cat >> "${PROJECT_FILE}" << EOT
  <ComputeNode Name="${node_name}">
EOT

  # Fill compute node information
  # shellcheck disable=SC2086
  _fill_compute_node_info ${args_to_pass}

  # Close compute node
  cat >> "${PROJECT_FILE}" << EOT
  </ComputeNode>

EOT
}

add_compute_node_on_top() {
  # Retrieve parameters
  local node_name=${1:-$DEFAULT_NODE_NAME}
  shift 1
  local args_to_pass=$*

  # Check parameters
  if [ -z "${node_name}" ]; then
    echo "[ERROR] Cannot add compute node because node_name is empty"
    exit 1
  fi

  # Dump information to file

  # Open compute node
  cat >> "${PROJECT_FILE}" << EOT
  <ComputeNode Name="${node_name}">
EOT

  # Fill compute node information
  # shellcheck disable=SC2086
  _fill_compute_node_info ${args_to_pass}

  # Close compute node
  cat >> "${PROJECT_FILE}" << EOT
  </ComputeNode>

EOT

  # Now add the worker in master on top of the project xml file
  FIRST_CN_OPEN=$(grep -n "ComputeNode" ${PROJECT_FILE} | cut -d ":" -f 1 | head -n 1)
  LAST_CN_OPEN=$(grep -n "ComputeNode" ${PROJECT_FILE} | cut -d ":" -f 1 | tail -n 2 | head -n 1)
  LAST_CN_CLOSE=$(grep -n "ComputeNode" ${PROJECT_FILE} | cut -d ":" -f 1 | tail -n 1)
  # Get the worker in master block
  WORKER_IN_MASTER_BLOCK=$(sed -n "${LAST_CN_OPEN},${LAST_CN_CLOSE}p" ${PROJECT_FILE})
  # Remove the worker in master block
  sed -i "${LAST_CN_OPEN},${LAST_CN_CLOSE}d" ${PROJECT_FILE}
  # Get header
  header=$(head -n $((FIRST_CN_OPEN-1)) ${PROJECT_FILE})
  # Get footer
  total_lines=$(wc -l < ${PROJECT_FILE})
  footer=$(tail -n $((total_lines-FIRST_CN_OPEN+1)) ${PROJECT_FILE})

  echo "${header}" > ${PROJECT_FILE}.tmp
  echo "${WORKER_IN_MASTER_BLOCK}" >> ${PROJECT_FILE}.tmp
  echo "${footer}" >> ${PROJECT_FILE}.tmp

  mv ${PROJECT_FILE}.tmp ${PROJECT_FILE}
}

add_cloud() {
  local init_vms=${1:-$DEFAULT_INIT_VMS}
  local min_vms=${2:-$DEFAULT_MIN_VMS}
  local max_vms=${3:-$DEFAULT_MAX_VMS}
  local cp_name=${4:-$DEFAULT_CLOUD_PROVIDER_NAME}
  local properties=${5:-$DEFAULT_CLOUD_PROPERTIES}
  local image_name=${6:-$DEFAULT_IMAGE_NAME}
  shift 6
  args_to_pass=$*
  local instance_types=${!#}

  # Check parameters
  if [ -z "${max_vms}" ]; then
    echo "[ERROR] Cannot add cloud node because max_vms is empty"
    exit 1
  fi
  if [ -z "${cp_name}" ]; then
    echo "[ERROR] Cannot add cloud node because cloud provider name is empty"
    exit 1
  fi
  if [ -z "${image_name}" ]; then
    echo "[ERROR] Cannot add cloud node because image_name is empty"
    exit 1
  fi
  if [ -z "${instance_types}" ]; then
    instance_types=${DEFAULT_INSTANCE_TYPES}
  fi

  # Open cloud tag
  cat >> "${PROJECT_FILE}" << EOT
  <Cloud>
    <InitialVMs>${init_vms}</InitialVMs>
    <MinimumVMs>${min_vms}</MinimumVMs>
    <MaximumVMs>${max_vms}</MaximumVMs>
    <CloudProvider Name="${cp_name}">
      <LimitOfVMs>${max_vms}</LimitOfVMs>
EOT

  # Add properties if defined
  if [ -n "${properties}" ]; then
    cat >> "${PROJECT_FILE}" << EOT
      <Properties>
EOT
    for prop in ${properties}; do
      p_name=$(echo "$prop" | cut -d "=" -f 1)
      p_value=${prop#"${p_name}="}
      p_value=$(echo ${p_value} | tr "%" "=" | tr "#" " ")
      cat >> "${PROJECT_FILE}" << EOT
        <Property>
          <Name>${p_name}</Name>
          <Value>${p_value}</Value>
        </Property>
EOT
    done
    cat >> "${PROJECT_FILE}" << EOT
      </Properties>
EOT
  fi

  # Add images
  # TODO: Add more than a single image
  cat >> "${PROJECT_FILE}" << EOT
      <Images>
        <Image Name="${image_name}">
EOT

  # shellcheck disable=SC2086
  _fill_compute_node_info ${args_to_pass}

  cat >> "${PROJECT_FILE}" << EOT
        </Image>
      </Images>
EOT

  # Add instances
  cat >> "${PROJECT_FILE}" << EOT
      <InstanceTypes>
EOT
  for it in ${instance_types}; do
    cat >> "${PROJECT_FILE}" << EOT
        <InstanceType Name="${it}"/>
EOT
  done
  cat >> "${PROJECT_FILE}" << EOT
      </InstanceTypes>
EOT

  # Close cloud tag
  cat >> "${PROJECT_FILE}" << EOT
    </CloudProvider>
  </Cloud>
EOT

}


#
# PRIVATE FUNCTIONS
#



_fill_processors() {
  # Retrieve parameters
  local cus=${1:-$DEFAULT_CUS}
  local gpus=${2:-$DEFAULT_GPUS}
  local fpgas=${3:-$DEFAULT_FPGAS}

  # Check parameters
  :

  # Dump information to file

  # TODO: Architecture as parameter
  # TODO: Speed as parameter
  if [ -n "${cus}" ] && [ "${cus}" -gt 0 ]; then
    cat >> "${PROJECT_FILE}" << EOT
    <Processor Name="MainProcessor">
      <ComputingUnits>${cus}</ComputingUnits>
      <Architecture>Intel</Architecture>
      <Speed>2.6</Speed>
    </Processor>
EOT
  fi
  if [ -n "${gpus}" ] && [ "${gpus}" -gt 0 ]; then
    cat >> "${PROJECT_FILE}" << EOT
    <Processor Name="GPU">
      <Type>GPU</Type>
      <ComputingUnits>${gpus}</ComputingUnits>
      <Architecture>k80</Architecture>
      <Speed>2.6</Speed>
    </Processor>
EOT
  fi
  if [ -n "${fpgas}" ] && [ "${fpgas}" -gt 0 ]; then
    cat >> "${PROJECT_FILE}" << EOT
    <Processor Name="FPGA">
      <Type>FPGA</Type>
      <ComputingUnits>${fpgas}</ComputingUnits>
      <Architecture>altera</Architecture>
      <Speed>1.0</Speed>
    </Processor>
EOT
  fi
}

_fill_memory() {
  # Retrieve parameters
  local memory=${1:-$DEFAULT_MEMORY}

  # Check parameters
  :

  # Dump information to file
  if [ -n "${memory}" ] && [ "${memory}" != "NULL" ]; then
    cat >> "${PROJECT_FILE}" << EOT
    <Memory>
      <Size>${memory}</Size>
    </Memory>
EOT
  fi
}



_fill_compute_node_info() {
  # Retrieve parameters
  local worker_install_dir=${1:-$DEFAULT_WORKER_INSTALL_DIR}
  local worker_working_dir=${2:-$DEFAULT_WORKER_WORKING_DIR}
  local user=${3:-$DEFAULT_USER}
  local app_dir=${4:-$DEFAULT_APP_DIR}
  local library_path=${5:-$DEFAULT_LIBRARY_PATH}
  local classpath=${6:-$DEFAULT_CLASSPATH}
  local pythonpath=${7:-$DEFAULT_PYTHONPATH}
  local lot=${8:-$DEFAULT_LIMIT_OF_TASKS}

  # Parameter checks
  if [ -z "${worker_install_dir}" ]; then
    echo "[ERROR] Cannot add compute node because worker_install_dir is empty"
    exit 1
  fi
  if [ -z "${worker_working_dir}" ]; then
    echo "[ERROR] Cannot add compute node because worker_working_dir is empty"
    exit 1
  fi

  # Dump information to file

  # Open compute node
  cat >> "${PROJECT_FILE}" << EOT
    <InstallDir>${worker_install_dir}</InstallDir>
    <WorkingDir>${worker_working_dir}</WorkingDir>
EOT

  # Add user if defined
  if [ -n "${user}" ] && [ "${user}" != "NULL" ]; then
    cat >> "${PROJECT_FILE}" << EOT
    <User>$user</User>
EOT
  fi

  # Add application section if defined
  init_app=0
  if [ -n "${app_dir}" ] && [ "${app_dir}" != "NULL" ]; then
    if [ ${init_app} -eq 0 ]; then
      cat >> "${PROJECT_FILE}" << EOT
    <Application>
EOT
      init_app=1
    fi
    cat >> "${PROJECT_FILE}" << EOT
      <AppDir>${app_dir}</AppDir>
EOT
  fi
  if [ -n "${library_path}" ] && [ "${library_path}" != "NULL" ]; then
    if [ ${init_app} -eq 0 ]; then
      cat >> "${PROJECT_FILE}" << EOT
    <Application>
EOT
      init_app=1
    fi
    cat >> "${PROJECT_FILE}" << EOT
      <LibraryPath>${library_path}</LibraryPath>
EOT
  fi
  if [ -n "${classpath}" ] && [ "${classpath}" != "NULL" ]; then
    if [ ${init_app} -eq 0 ]; then
      cat >> "${PROJECT_FILE}" << EOT
    <Application>
EOT
      init_app=1
    fi
    cat >> "${PROJECT_FILE}" << EOT
      <Classpath>${classpath}</Classpath>
EOT
  fi
  if [ -n "${pythonpath}" ] && [ "${pythonpath}" != "NULL" ]; then
    if [ ${init_app} -eq 0 ]; then
      cat >> "${PROJECT_FILE}" << EOT
    <Application>
EOT
      init_app=1
    fi
    cat >> "${PROJECT_FILE}" << EOT
      <Pythonpath>${pythonpath}</Pythonpath>
EOT
  fi
  if [ ${init_app} -ne 0 ]; then
    cat >> "${PROJECT_FILE}" << EOT
    </Application>
EOT
  fi

  # Add limit of tasks if defined
  if [ -n "$lot" ] && [ "$lot" != "NULL" ] && [ "$lot" -ge 0 ]; then
    cat >> "${PROJECT_FILE}" <<EOT
    <LimitOfTasks>${lot}</LimitOfTasks>
EOT
  fi

  # TODO: SKIPPING ADAPTORS SECTION
}


#
# MAIN FUNCTION FOR SIMPLE PROJECT CREATION
#

create_simple_project() {
  # Function called by the Runtime when executing COMPSs Nested

  local project=$1
  local master_info=$2  # "name:cus:install_dir:working_dir ..."
  local workers_info=$3  # "name:cus:install_dir:working_dir ..."
                         # Some parameters are used by the resources generation and skiped here

  init "${project}"
  add_header
  IFS=":" read -ra master_info_fields <<< "${master_info}"
  local master_cus=${master_info_fields[1]}
  add_master_node "${master_cus}" "0" "0" "NULL"

  for worker_info in ${workers_info}; do
    IFS=":" read -ra worker_info_fields <<< "${worker_info}"
    local worker_name=${worker_info_fields[0]}
    #local worker_cus=${worker_info_fields[1]}
    local worker_install_dir=${worker_info_fields[2]}
    local worker_working_dir=${worker_info_fields[3]}
    add_compute_node "${worker_name}" "${worker_install_dir}" "${worker_working_dir}" "" "" "" "" "" ""
  done
  add_footer
}


#
# MAIN (when script is called directly)
#

if [ $# -ne 0 ]; then
  #to check if it is sourced or executed
  if [ "$(basename "$0")" = "$(basename "$BASH_SOURCE")" ]; then
      create_simple_project "$@"
  else
      echo "Sourcing generate_project.sh"
  fi
fi
