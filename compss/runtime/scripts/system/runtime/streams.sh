#!/bin/bash

if [ -n "${LOADED_SYSTEM_RUNTIME_STREAMS}" ]; then
  return 0
fi

# Checking up COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi

# Load auxiliar scripts

# shellcheck source=../system/commons/logger.sh
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/system/commons/logger.sh"

###############################################
###############################################
#            CONSTANTS DEFINITION
###############################################
###############################################

#----------------------------------------------
# DEFAULT VALUES
#----------------------------------------------
#   Streaming back-end platform
STREAMING_OFF="NONE"
STREAMING_OBJECTS="OBJECTS"
STREAMING_FILES="FILES"
STREAMING_ALL="ALL"

DEFAULT_STREAMING=${STREAMING_OFF}

#   DistroStream Server port (Base + rand([0, RANGE]))
#     Base port
BASE_STREAMING_PORT=49049
#     Range of ports
STREAMING_PORT_RAND_RANGE=100

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------
STREAM_BACKEND_ERROR="ERROR: Cannot load stream backend. Invalid KAFKA_HOME location"
ERROR_ZOOKEEPER_CONFIG="ERROR: Cannot create zookeeper configuration file"
ERROR_KAFKA_CONFIG="ERROR: Cannot create kafka configuration file"

###############################################
###############################################
#        STREAM HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK STREAMING-RELATED ENV VARIABLES
#----------------------------------------------
check_stream_env() {
  if [ -z "${KAFKA_HOME}" ]; then
    KAFKA_HOME=${COMPSS_HOME}/Dependencies/kafka/
    export KAFKA_HOME=${KAFKA_HOME}
  fi
}


#----------------------------------------------
# CHECK STREAMING-RELATED SETUP values
#----------------------------------------------
check_stream_setup () {
  if [ -z "${streaming}" ]; then
    streaming=${DEFAULT_STREAMING}
  fi
  if [ "${streaming}" != "null" ] && [ "${streaming}" != "NONE" ]; then
    if [ -z "${streaming_master_name}" ]; then
      if [ -z "${master_name}" ]; then
        streaming_master_name="null"
      else
        streaming_master_name=${master_name}
      fi
    fi
    if [ -z "${streaming_master_port}" ]; then
      streaming_master_port=$((BASE_STREAMING_PORT + RANDOM % STREAMING_PORT_RAND_RANGE))
    fi
  else
    streaming_master_name="null"
    streaming_master_port="null"
  fi
}

#----------------------------------------------
# GENERATE CONFIGURATION FILES
# Output variables:
# zookeeper_props_file <- zookeeper configuration filepath
# kafka_props_file <- kafka configuration filepath
#----------------------------------------------
generate_stream_config_files() {
  # Create zookeeper properties
  zookeeper_log_dir="/tmp/zookeeper"
  rm -rf "${zookeeper_log_dir}"
  mkdir -p "${zookeeper_log_dir}"
  zookeeper_props_file=$(mktemp -p "${zookeeper_log_dir}") || fatal_error "${ERROR_ZOOKEEPER_CONFIG}" 1
  cat > "${zookeeper_props_file}" << EOT
dataDir=${zookeeper_log_dir}
clientPort=49000
maxClientCnxns=0
admin.enableServer=false
# admin.serverPort=8080
EOT

  # Create kafka properties
  kafka_log_dir="/tmp/kafka-logs"
  rm -rf "${kafka_log_dir}"
  mkdir -p "${kafka_log_dir}"
  kafka_props_file=$(mktemp -p "${kafka_log_dir}") || fatal_error "${ERROR_KAFKA_CONFIG}" 1
  cat > "${kafka_props_file}" << EOT
broker.id=$((RANDOM % 100))
port=49001
num.network.threads=3
num.io.threads=8
socket.send.buffer.bytes=102400
socket.receive.buffer.bytes=102400
socket.request.max.bytes=104857600
log.dirs=/tmp/kafka-logs
num.partitions=1
num.recovery.threads.per.data.dir=1
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
#log.flush.interval.messages=10000
#log.flush.interval.ms=1000
log.retention.hours=168
log.segment.bytes=1073741824
#log.retention.bytes=1073741824
log.retention.check.interval.ms=300000
zookeeper.connect=localhost:49000
zookeeper.connection.timeout.ms=18000
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=true
max.block.ms=600000
listeners=PLAINTEXT://:49001
# advertised.listeners=PLAINTEXT://localhost:49001
EOT
}

#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_stream_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.streaming=${streaming}
-Dcompss.streaming.masterName=${streaming_master_name}
-Dcompss.streaming.masterPort=${streaming_master_port}
EOT
}

#----------------------------------------------
# STARTS THE STREAMING BACK-ENDS
#----------------------------------------------
start_stream_backends() {
  if [ "${streaming}" == "${STREAMING_OBJECTS}" ] || [ "${streaming}" == "${STREAMING_ALL}" ]; then
  echo ""
  display_info "Starting Streaming Backend"
    if [ -d "${KAFKA_HOME}" ]; then
      # Generate stream configuration files
      generate_stream_config_files
      # Clean classpath before starting daemons
      local backup_classpath="$CLASSPATH"
      local backup_log_dir="${LOG_DIR}"
      export CLASSPATH=

      # Start ZooKeeper and Kafka
      export LOG_DIR="${zookeeper_log_dir}"
      # echo "ZK: ${KAFKA_HOME}/bin/zookeeper-server-start.sh -daemon ${zookeeper_props_file}"
      "${KAFKA_HOME}"/bin/zookeeper-server-start.sh -daemon "${zookeeper_props_file}"
      sleep 1s
      export LOG_DIR="${kafka_log_dir}"
      # echo "KAFKA: ${KAFKA_HOME}/bin/kafka-server-start.sh -daemon ${kafka_props_file}"
      "${KAFKA_HOME}"/bin/kafka-server-start.sh -daemon "${kafka_props_file}"

      # Wait for servers to setup
      sleep 5s

      # Restore classpath
      export CLASSPATH=${backup_classpath}
      export LOG_DIR=${backup_log_dir}
    else
      fatal_error "${STREAM_BACKEND_ERROR}" 1
    fi
    display_info "Streaming Backend ready"
  fi
}

#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_stream_env () {
  if [ "${streaming}" == "${STREAMING_OBJECTS}" ] || [ "${streaming}" == "${STREAMING_ALL}" ]; then
    "${KAFKA_HOME}"/bin/kafka-server-stop.sh
    "${KAFKA_HOME}"/bin/zookeeper-server-stop.sh

    # Delete ZooKeeper and Kafka logs and configuration files
    rm -rf "${zookeeper_log_dir}"
    rm -rf "${kafka_log_dir}"
  fi
}

LOADED_SYSTEM_RUNTIME_STREAMS=1
