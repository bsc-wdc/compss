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
DEFAULT_IMAGE_NAME=""
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
  local shared_disks=$*

  # Check parameters
  :

  # Dump information to file
  if [ -n "${shared_disks}" ] && [ "${shared_disks}" != "NULL" ]; then
    # Create master node with shared disks
    cat >> "${PROJECT_FILE}" << EOT
  <MasterNode>
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
  </MasterNode>

EOT

  else
    # Create empty master node
    cat >> "${PROJECT_FILE}" << EOT
  <MasterNode></MasterNode>

EOT
  fi

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
      p_name=${prop%=*}  # Deletes shortest =* from back
      p_value=${prop##*=}  # Deletes longest *= from front
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
  local workers_info=$2  # "name:cus:install_dir:working_dir ..."
                         # Some parameters are used by the resources generation and skiped here

  init "${project}"
  add_header
  add_master_node ""
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
  create_simple_project "$@"
fi

