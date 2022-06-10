#!/bin/bash

if [ -n "${LOADED_SYSTEM_AGENTS_COMMONS}" ]; then
  return 0
fi

# Checking up COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi

# Load auxiliar scripts

###############################################
# COMPSs Default values re-definitions
###############################################
DEFAULT_COMMUNICATION_ADAPTOR=${COMM_AGENT_ADAPTOR}

DEFAULT_RESOURCES="${COMPSS_HOME}Runtime/configuration/xml/resources/examples/local/resources.xml"
DEFAULT_PROJECT="${COMPSS_HOME}Runtime/configuration/xml/projects/examples/local/project.xml"

###############################################
# Default values definitions
###############################################
DEFAULT_AGENT_NODE="localhost"
DEFAULT_AGENT_PORT="46101"

DEFAULT_CPU_NAME="MainProcessor"
DEFAULT_CPU_COUNT="1"
DEFAULT_GPU_COUNT="0"
DEFAULT_FPGA_COUNT="0"
DEFAULT_MEM_TYPE="[unassigned]"
DEFAULT_MEM_SIZE="-1"
DEFAULT_OS_TYPE="[unassigned]"
DEFAULT_OS_DISTR="[unassigned]"
DEFAULT_OS_VERSION="[unassigned]"


###############################################
# Resource description
###############################################

# Creates the xml element for the properties for the adaptor
# Input: 
#   list of key=value pairs that contain the properties of the adaptor
# Return :
#   ADAPTOR_PROPERTIES_XML <= XML element with the properties configuration
#   ADAPTOR_PROPERTIES_DEBUG <= String with the name and value of the detected properties. 
get_adaptor_properties() {
  ADAPTOR_PROPERTIES_DEBUG=""
  ADAPTOR_PROPERTIES_XML="<resourceConf xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"ResourcesExternalAdaptorProperties\">"

  for property in "${@}"; do
    property_name=$(echo "${property}" | cut -d '=' -f1)
    property_value=$(echo "${property}" | cut -d '=' -f2)
    ADAPTOR_PROPERTIES_DEBUG="${ADAPTOR_PROPERTIES_DEBUG}                              ${property_name} -> ${property_value}
"
    ADAPTOR_PROPERTIES_XML="${ADAPTOR_PROPERTIES_XML}
      <Property>
        <Name>${property_name}</Name>
        <Value>${property_value}</Value>
      </Property>"
  done

  ADAPTOR_PROPERTIES_XML="${ADAPTOR_PROPERTIES_XML}
    </resourceConf>"
}


# Creates the XML element containing the description of one node
get_resource_description() {
  local indent=${1}
  local cpu_name=${2}
  local cpu_count=${3}
  local gpu_count=${4}
  local fpga_count=${5}
  local mem_size=${6}
  local mem_type=${7}
  local os_distr=${8}
  local os_type=${9}
  local os_version=${10}

  local processors
  processors="${indent}  <processors>"
  if [ "${cpu_count}" -gt "0" ]; then
    processors="${processors}
${indent}    <processor>
${indent}      <name>${cpu_name}</name>
${indent}      <type>CPU</type>
${indent}      <architecture>[unassigned]</architecture>
${indent}      <computingUnits>${cpu_count}</computingUnits>
${indent}      <internalMemory>-1.0</internalMemory>
${indent}      <propName>[unassigned]</propName>
${indent}      <propValue>[unassigned]</propValue>
${indent}      <speed>-1.0</speed>
${indent}    </processor>"
  fi

  if [ "${gpu_count}" -gt "0" ]; then
    processors="${processors}
${indent}    <processor>
${indent}      <name>gpu</name>
${indent}      <type>GPU</type>
${indent}      <architecture>[unassigned]</architecture>
${indent}      <computingUnits>${gpu_count}</computingUnits>
${indent}      <internalMemory>-1.0</internalMemory>
${indent}      <propName>[unassigned]</propName>
${indent}      <propValue>[unassigned]</propValue>
${indent}      <speed>-1.0</speed>
${indent}    </processor>"
  fi

  if [ "${fpga_count}" -gt "0" ]; then
    processors="${processors}
${indent}    <processor>
${indent}      <name>FPGA</name>
${indent}      <type>FPGA</type>
${indent}      <architecture>[unassigned]</architecture>
${indent}      <computingUnits>${fpga_count}</computingUnits>
${indent}      <internalMemory>-1.0</internalMemory>
${indent}      <propName>[unassigned]</propName>
${indent}      <propValue>[unassigned]</propValue>
${indent}      <speed>-1.0</speed>
${indent}    </processor>"
  fi
    processors="${processors}
${indent}  </processors>"

  export RESOURCE_DESCRIPTION="${processors}
${indent}  <memorySize>${mem_size}</memorySize>
${indent}  <memoryType>${mem_type}</memoryType>
${indent}  <storageSize>-1.0</storageSize>
${indent}  <storageType>[unassigned]</storageType>
${indent}  <operatingSystemDistribution>${os_distr}</operatingSystemDistribution>
${indent}  <operatingSystemType>${os_type}</operatingSystemType>
${indent}  <operatingSystemVersion>${os_version}</operatingSystemVersion>
${indent}  <pricePerUnit>-1.0</pricePerUnit>
${indent}  <priceTimeUnit>-1</priceTimeUnit>
${indent}  <value>0.0</value>
${indent}  <wallClockLimit>-1</wallClockLimit>"
}


###############################################
# Function to get the arguments
###############################################
get_parameters(){
  PARAMETERS="<parameters>"
  param_id=0
  local lang=${1}
  shift 1
  for param in "$@"; do
    PARAMETERS="${PARAMETERS}
      <params paramId=\"${param_id}\">
        <direction>IN</direction>
        <paramName></paramName>
        <prefix></prefix>
        <contentType></contentType>
        <stdIOStream>UNSPECIFIED</stdIOStream>
        <type>STRING_64_T</type>
        <element paramId=\"${param_id}\">
          <className>java.lang.String</className>
          <value xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xsi:type=\"xs:string\">"
  if [ "${lang}"  == "JAVA" ] || [ "${lang}"  == "java" ]; then  
    PARAMETERS="${PARAMETERS}${param}"
  elif [ "${lang}"  == "PYTHON" ] || [ "${lang}"  == "python" ]; then
    encoded_value=$(python3 -c "import base64; print( str(base64.b64encode(('#' +'${param}').encode()).decode()))")
    PARAMETERS="${PARAMETERS}${encoded_value}"
  elif [ "${lang}"  == "C" ] || [ "${lang}"  == "c" ]; then
    PARAMETERS="${PARAMETERS}${param}"
  fi
  PARAMETERS="${PARAMETERS}</value>
        </element>
      </params>"
    param_id=$((param_id + 1))
  done
  PARAMETERS="${PARAMETERS}
    </parameters>"
}

get_parameters_as_array(){
  local lang=${1}
  shift 1
  PARAMETERS="<parameters>
      <params paramId=\"0\">
        <direction>IN</direction>
        <paramName>args</paramName>
        <prefix></prefix>
        <contentType></contentType>
        <stdIOStream>UNSPECIFIED</stdIOStream>
        <type>OBJECT_T</type>
        <array paramId=\"0\">
          <componentClassname>java.lang.String</componentClassname>
          <values>"

  param_id=0
  for param in "$@"; do
    PARAMETERS="${PARAMETERS}
            <element paramId=\"${param_id}\">
              <className>java.lang.String</className>
              <value xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xsi:type=\"xs:string\">"
    if [ "${lang}"  == "JAVA" ] || [ "${lang}"  == "java" ]; then  
      PARAMETERS="${PARAMETERS}${param}"
    elif [ "${lang}"  == "PYTHON" ] || [ "${lang}"  == "python" ]; then
      encoded_value=$(python3 -c "import base64; print( str(base64.b64encode(('#' +'${param}').encode()).decode()))")
      PARAMETERS="${PARAMETERS}${encoded_value}"
    elif [ "${lang}"  == "C" ] || [ "${lang}"  == "c" ]; then
      PARAMETERS="${PARAMETERS}${param}"
    fi
    PARAMETERS="${PARAMETERS}</value>
            </element>"
    param_id=$((param_id + 1))
  done

  PARAMETERS="${PARAMETERS}
          </values>
        </array>
      </params>
    </parameters>"
}

LOADED_SYSTEM_AGENTS_COMMONS=1