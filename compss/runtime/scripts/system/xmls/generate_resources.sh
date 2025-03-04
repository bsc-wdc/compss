#!/bin/bash

#
# SCRIPT CONSTANTS
#

DEFAULT_RESOURCES_FILENAME="resources.xml"

DEFAULT_NODE_NAME=""
DEFAULT_CUS=""
DEFAULT_GPUS=""
DEFAULT_FPGAS=""
DEFAULT_MEMORY=""
DEFAULT_NODE_STORAGE_BANDWIDTH=""
DEFAULT_MIN_PORT="43001"
DEFAULT_MAX_PORT="43002"
DEFAULT_REMOTE_EXECUTOR=""
DEFAULT_SHARED_DISKS=""

DEFAULT_CLOUD_PROVIDER_NAME=""
DEFAULT_SERVER=""
DEFAULT_CONNECTOR_JAR=""
DEFAULT_CONNECTOR_CLASS=""
DEFAULT_IMAGE_NAME="None"
DEFAULT_CREATION_TIME=""
DEFAULT_REMOTE_EXECUTOR=""
DEFAULT_INSTANCE_TYPES=""
DEFAULT_IT_NAME=""
DEFAULT_IT_TIME=""
DEFAULT_IT_PRICE=""


#
# INITIALIZATION
#
init() {
  RESOURCES_FILE=${1:-$DEFAULT_RESOURCES_FILENAME}
}


#
# PUBLIC FUNCTIONS
#

add_header() {
  cat > "${RESOURCES_FILE}" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ResourcesList>
EOT
}

add_footer() {
  cat >> "${RESOURCES_FILE}" << EOT
</ResourcesList>
EOT
}

add_shared_disks() {
  local sd_names=$*

  # Check parameters
  :

  # Dump information to file
  if [ -n "${sd_names}" ] && [ "${sd_names}" != "NULL" ]; then
    for sd in ${sd_names}; do
      add_shared_disk "$sd"
    done
  fi
}

add_shared_disk() {
  # Retrieve parameters
  local sd_name=${1:-DEFAULT_SHARED_DISK_NAME}

  # Check arguments
  :

  # Dump information into file
  cat >> "${RESOURCES_FILE}" << EOT
  <SharedDisk Name="${sd_name}" />
EOT
}

add_compute_node() {
  # Retrieve parameters
  local node_name=${1:-$DEFAULT_NODE_NAME}
  shift 1

  # Check parameters
  if [ -z "${node_name}" ]; then
    echo "[ERROR] Cannot add compute node because node_name is empty"
    exit 1
  fi

  # Dump information to file

  # Open compute node tag
  cat >> "${RESOURCES_FILE}" << EOT
  <ComputeNode Name="${node_name}">
EOT

  _fill_hw "${@:1:5}"
  shift 5

  _fill_sw "$@"

  # Close compute node tag
  cat >> "${RESOURCES_FILE}" << EOT
  </ComputeNode>

EOT
}

add_compute_node_on_top() {
  # Retrieve parameters
  local node_name=${1:-$DEFAULT_NODE_NAME}
  shift 1

  # Check parameters
  if [ -z "${node_name}" ]; then
    echo "[ERROR] Cannot add compute node because node_name is empty"
    exit 1
  fi

  # Dump information to file

  # Open compute node tag
  cat >> "${RESOURCES_FILE}" << EOT
  <ComputeNode Name="${node_name}">
EOT

  _fill_hw "${@:1:5}"
  shift 5

  _fill_sw "$@"

  # Close compute node tag
  cat >> "${RESOURCES_FILE}" << EOT
  </ComputeNode>

EOT

  # Now add the worker in master on top of the resources xml file
  FIRST_CN_OPEN=$(grep -n "ComputeNode" ${RESOURCES_FILE} | cut -d ":" -f 1 | head -n 1)
  LAST_CN_OPEN=$(grep -n "ComputeNode" ${RESOURCES_FILE} | cut -d ":" -f 1 | tail -n 2 | head -n 1)
  LAST_CN_CLOSE=$(grep -n "ComputeNode" ${RESOURCES_FILE} | cut -d ":" -f 1 | tail -n 1)
  # Get the worker in master block
  WORKER_IN_MASTER_BLOCK=$(sed -n "${LAST_CN_OPEN},${LAST_CN_CLOSE}p" ${RESOURCES_FILE})
  # Remove the worker in master block
  sed -i "${LAST_CN_OPEN},${LAST_CN_CLOSE}d" ${RESOURCES_FILE}
  # Get header
  header=$(head -n $((FIRST_CN_OPEN-1)) ${RESOURCES_FILE})
  # Get footer
  total_lines=$(wc -l < ${RESOURCES_FILE})
  footer=$(tail -n $((total_lines-FIRST_CN_OPEN+1)) ${RESOURCES_FILE})

  echo "${header}" > ${RESOURCES_FILE}.tmp
  echo "${WORKER_IN_MASTER_BLOCK}" >> ${RESOURCES_FILE}.tmp
  echo "${footer}" >> ${RESOURCES_FILE}.tmp

  mv ${RESOURCES_FILE}.tmp ${RESOURCES_FILE}
}

add_cloud() {
  # Retrieve parameters
  local cp_name=${1:-$DEFAULT_CLOUD_PROVIDER_NAME}
  local server=${2:-$DEFAULT_SERVER}
  local connector_jar=${3:-$DEFAULT_CONNECTOR_JAR}
  local connector_class=${4:-$DEFAULT_CONNECTOR_CLASS}
  local image_name=${5:-$DEFAULT_IMAGE_NAME}
  local shared_disks=${6:-$DEFAULT_SHARED_DISKS}
  local creation_time=${7:-$DEFAULT_CREATION_TIME}
  local min_port=${8:-$DEFAULT_MIN_PORT}
  local max_port=${9:-$DEFAULT_MAX_PORT}
  local remote_executor=${10:-$DEFAULT_REMOTE_EXECUTOR}
  local instance_types=${11:-$DEFAULT_INSTANCE_TYPES}

  # Check parameters
  if [ -z "${cp_name}" ]; then
    echo "[ERROR] Cannot add cloud node because cloud provider name is empty"
    exit 1
  fi

  if [ -z "${connector_jar}" ]; then
      echo "[ERROR] Cannot add cloud node because connector_jar is empty"
      exit 1
  fi

  if [ -z "${connector_class}" ]; then
    echo "[ERROR] Cannot add cloud node because connector_class is empty"
    exit 1
  fi

  if [ -z "${creation_time}" ]; then
    echo "[ERROR] Cannot add compute node because creation_time is empty"
    exit 1
  fi

  # Dump information to file

  # Add cloud provider tag and general information
  # Add images tag
  # TODO: Add multiple images
  cat >> "${RESOURCES_FILE}" << EOT
  <CloudProvider Name="${cp_name}">
    <Endpoint>
EOT
  if [ -n "${server}" ] && [ "${server}" != "NULL" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
      <Server>${server}</Server>
EOT
  fi
  cat >> "${RESOURCES_FILE}" << EOT
      <ConnectorJar>${connector_jar}</ConnectorJar>
      <ConnectorClass>${connector_class}</ConnectorClass>
    </Endpoint>

    <Images>
EOT

  # Add image information
  cat >> "${RESOURCES_FILE}" << EOT
      <Image Name="${image_name}">
        <CreationTime>${creation_time}</CreationTime>
EOT
  _fill_os_info
  _fill_software_info
  _fill_adaptors "${min_port}" "${max_port}" "${remote_executor}"
  _fill_shared_disks "${shared_disks}"

  # Close image and images tags
  cat >> "${RESOURCES_FILE}" << EOT
      </Image>
    </Images>

EOT

  # Add instrance type tag
  cat >> "${RESOURCES_FILE}" << EOT
    <InstanceTypes>
EOT

  for it in ${instance_types}; do
    IFS=":" read -ra it_fields <<< "${it}"
    local it_name=${it_fields[0]:-$DEFAULT_IT_NAME}
    local it_cus=${it_fields[1]:-$DEFAULT_CUS}
    local it_gpus=${it_fields[2]:-$DEFAULT_GPUS}
    local it_fpgas=${it_fields[3]:-$DEFAULT_FPGAS}
    local it_memory=${it_fields[4]:-$DEFAULT_MEMORY}
    local it_time=${it_fields[5]:-$DEFAULT_IT_TIME}
    local it_price=${it_fields[6]:-$DEFAULT_IT_PRICE}

    if [ -z "${it_name}" ]; then
      echo "[ERROR] Cannot add cloud node because it_name is empty"
      exit 1
    fi
    if [ -z "${it_time}" ]; then
      echo "[ERROR] Cannot add cloud node because it_time is empty"
      exit 1
    fi
    if [ -z "${it_price}" ]; then
      echo "[ERROR] Cannot add cloud node because it_price is empty"
      exit 1
    fi

    cat >> "${RESOURCES_FILE}" << EOT
      <InstanceType Name="${it_name}">
EOT
    _fill_processors "${it_cus}" "${it_gpus}" "${it_fpgas}"
    _fill_memory "${it_memory}"
    cat >> "${RESOURCES_FILE}" << EOT
          <Price>
            <TimeUnit>${it_time}</TimeUnit>
            <PricePerUnit>${it_price}</PricePerUnit>
          </Price>
      </InstanceType>
EOT
  done

  # Close instance type tag
  cat >> "${RESOURCES_FILE}" << EOT
    </InstanceTypes>
EOT

  # Close cloud provider tag
  cat >> "${RESOURCES_FILE}" << EOT
  </CloudProvider>
EOT
}


#
# PRIVATE FUNCTIONS
#

_fill_hw() {
  _fill_processors "$@"
  _fill_memory "${4}"
  _fill_storage "${!#}"
}

_fill_sw() {
  _fill_os_info
  _fill_software_info
  _fill_adaptors "$@"
  _fill_shared_disks "${!#}"
}

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
    cat >> "${RESOURCES_FILE}" << EOT
    <Processor Name="MainProcessor">
      <ComputingUnits>${cus}</ComputingUnits>
      <Architecture>Intel</Architecture>
      <Speed>2.6</Speed>
    </Processor>
EOT
  fi
  if [ -n "${gpus}" ] && [ "${gpus}" -gt 0 ]; then
    cat >> "${RESOURCES_FILE}" << EOT
    <Processor Name="GPU">
      <Type>GPU</Type>
      <ComputingUnits>${gpus}</ComputingUnits>
      <Architecture>k80</Architecture>
      <Speed>2.6</Speed>
    </Processor>
EOT
  fi
  if [ -n "${fpgas}" ] && [ "${fpgas}" -gt 0 ]; then
    cat >> "${RESOURCES_FILE}" << EOT
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
    cat >> "${RESOURCES_FILE}" << EOT
    <Memory>
      <Size>${memory}</Size>
    </Memory>
EOT
  fi
}

_fill_storage() {
  # Retrieve parameters
  local bandwidth=${1:-$DEFAULT_STORAGE_BANDWIDTH}

  # Check parameters
  :

  # Dump information to file
  if [ -n "${bandwidth}" ] && [ "${bandwidth}" != "NULL" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
    <Storage>
      <Bandwidth>${bandwidth}</Bandwidth>
    </Storage>
EOT
  fi
}

_fill_os_info() {
  # Retrieve parameters
  # TODO: OS Info as parameter

  # Check parameters
  :

  # Dump information to file
  cat >> "${RESOURCES_FILE}" << EOT
    <OperatingSystem>
      <Type>Linux</Type>
      <Distribution>SMP</Distribution>
      <Version>3.0.101-0.35-default</Version>
    </OperatingSystem>
EOT
}

_fill_software_info() {
  # Retrieve parameters
  # TODO: Software info as parameter

  # Check parameters
  :

  # Dump information to file
  cat >> "${RESOURCES_FILE}" << EOT
    <Software>
      <Application>JAVA</Application>
      <Application>PYTHON</Application>
      <Application>EXTRAE</Application>
      <Application>COMPSS</Application>
    </Software>
EOT
}

_fill_adaptors() {
  # Retrieve parameters
  local min_port=${1:-$DEFAULT_MIN_PORT}
  local max_port=${2:-$DEFAULT_MAX_PORT}
  local remote_executor=${3:-$DEFAULT_REMOTE_EXECUTOR}

  # Check parameters
  if [ -z "${min_port}" ]; then
    echo "[ERROR] Cannot add compute node because min_port is empty"
    exit 1
  fi

  if [ -z "${max_port}" ]; then
    echo "[ERROR] Cannot add compute node because max_port is empty"
    exit 1
  fi

  # Dump information to file
  cat >> "${RESOURCES_FILE}" << EOT
    <Adaptors>
      <Adaptor Name="es.bsc.compss.nio.master.NIOAdaptor">
        <SubmissionSystem>
          <Interactive/>
        </SubmissionSystem>
        <Ports>
          <MinPort>${min_port}</MinPort>
          <MaxPort>${max_port}</MaxPort>
EOT
   if [ -n "${remote_executor}" ] && [ "${remote_executor}" != "NULL" ]; then
     cat >> "${RESOURCES_FILE}" << EOT
          <RemoteExecutionCommand>${remote_executor}</RemoteExecutionCommand>
EOT
   fi
   cat >> "${RESOURCES_FILE}" << EOT
        </Ports>
      </Adaptor>
      <Adaptor Name="es.bsc.compss.gat.master.GATAdaptor">
        <SubmissionSystem>
          <Interactive/>
        </SubmissionSystem>
        <BrokerAdaptor>sshtrilead</BrokerAdaptor>
      </Adaptor>
      <Adaptor Name="es.bsc.compss.agent.comm.CommAgentAdaptor">
        <SubmissionSystem>
          <Interactive/>
        </SubmissionSystem>
        <Properties>
          <Property>
            <Name>Port</Name>
            <Value>46102</Value>
          </Property>
        </Properties>
      </Adaptor>
    </Adaptors>
EOT
}

_fill_shared_disks() {
  # Retrieve parameters
  local shared_disks=${1:-$DEFAULT_SHARED_DISKS}

  # Check parameters
  :

  # Dump information to file
  if [ -n "${shared_disks}" ] && [ "${shared_disks}" != "NULL" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
    <SharedDisks>
EOT
    for sd in ${shared_disks}; do
      sd_name=${sd%=*}  # Deletes shortest =* from back
      sd_mountpoint=${sd##*=}  # Deletes longest *= from front
      cat >> "${RESOURCES_FILE}" << EOT
      <AttachedDisk Name="${sd_name}">
        <MountPoint>${sd_mountpoint}</MountPoint>
      </AttachedDisk>
EOT
    done
    cat >> "${RESOURCES_FILE}" << EOT
    </SharedDisks>
EOT
  fi
}


#
# MAIN FUNCTION FOR SIMPLE RESOURCES CREATION
#

create_simple_resources() {
  # Function called by the Runtime when executing COMPSs Nested

  local resources=$1
  local workers_info=$2  # "name:cus:install_dir:working_dir ..."
                         # Some parameters are used by the project generation and skiped here

  init "${resources}"
  add_header
  for worker_info in ${workers_info}; do
    IFS=":" read -ra worker_info_fields <<< "${worker_info}"
    local worker_name=${worker_info_fields[0]}
    local worker_cus=${worker_info_fields[1]}
    #local worker_install_dir=${worker_info_fields[2]}
    #local worker_working_dir=${worker_info_fields[3]}
    min_port=$((43101 + ( RANDOM % 100 )))
    max_port=$((min_port + 1))
    add_compute_node "${worker_name}" "${worker_cus}" "0" "0" "" "" "${min_port}" "${max_port}" "" ""
  done
  add_footer
}


#
# MAIN (when script is called directly)
#

if [ $# -ne 0 ]; then
  create_simple_resources "$@"
fi
