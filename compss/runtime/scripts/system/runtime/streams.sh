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
# Unique identifier for the Kafka broker (randomly assigned within a range 0-99)
broker.id=$((RANDOM % 100))
# The port on which the Kafka broker will listen for incoming connections
port=49001
# Specifies the listener protocol (PLAINTEXT) and port (49001) to listen on
listeners=PLAINTEXT://:49001
# Define how brokers advertise themselves to clients. Commented out to use default
# advertised.listeners=PLAINTEXT://localhost:49001
# The level of acknowledgment required from the Kafka brokers for a message to be considered successfully written (all means all replicas must acknowledge)
acks=all
# If true, Kafka will automatically create topics when they are first referenced (not recommended for production)
auto.create.topics.enable=true
# The batch size in bytes for Kafka producer to send data in a single request (16 KB)
batch.size=8192
# Total memory available to Kafka producers for buffering data before it is sent to brokers (32 MB)
buffer.memory=33554432
# Compression algorithm used for Kafka producer messages
compression.type=gzip
# Ensure that messages are delivered exactly once by the producer (important for data integrity)
enable.idempotence=true
# The maximum amount of time in milliseconds the server will wait before responding to a fetch request
fetch.max.wait.ms=500
# The minimum number of bytes a broker should send back in a fetch response
fetch.min.bytes=1
# Time in milliseconds to wait before starting a rebalance in a consumer group after new members join
group.initial.rebalance.delay.ms=0
# The time in milliseconds to wait before sending a batch of messages (can be used to accumulate more messages in a batch)
linger.ms=100
# The time in milliseconds to retain log segments after they have been deleted by the log cleaner (1 day)
log.cleaner.delete.retention.ms=86400000
# Whether log cleaning (compaction) is enabled or not (used in compacted topics)
log.cleaner.enable=true
# The minimum ratio of a log segment that must be eligible for compaction before cleaning starts
log.cleaner.min.cleanable.ratio=0.5
# Directory where Kafka will store its logs (data storage location for topics)
log.dirs=/tmp/kafka-logs
# The number of messages that must be written to a log before it is flushed to disk
log.flush.interval.messages=10000
# The interval in milliseconds after which logs are flushed to disk
log.flush.interval.ms=5000
# Maximum size of logs to retain in bytes (1 GB)
log.retention.bytes=1073741824
# The interval in milliseconds for Kafka to check whether any logs need to be deleted
log.retention.check.interval.ms=300000
# The retention time of logs in hours (2 days) before they are deleted
log.retention.hours=48
# Maximum size of a log segment file in bytes (1 GB)
log.segment.bytes=1073741824
# Maximum time in milliseconds a producer can block waiting for a response from the broker
max.block.ms=600000
# Maximum number of records that a consumer can fetch in a single poll request
max.poll.records=100
# Minimum number of in-sync replicas needed to acknowledge a write
min.insync.replicas=1
# Number of threads to handle disk I/O operations
num.io.threads=10
# Number of threads to handle network requests
num.network.threads=5
# Default number of partitions for new topics (each partition is a log segment)
num.partitions=1
# Number of threads to recover logs from each directory during broker start-up
num.recovery.threads.per.data.dir=1
# Replication factor for the offsets topic (1 means no redundancy)
offsets.topic.replication.factor=1
# Default replication factor for new topics
replication.factor=2
# The maximum number of times the producer will retry sending a message after failure (set to a very high number)
retries=2147483647
# The time in milliseconds to wait before retrying a failed request
retry.backoff.ms=200
# The size of the socket receive buffer for network communication
socket.receive.buffer.bytes=102400
# Maximum number of bytes a broker will accept in a single request (100 MB)
socket.request.max.bytes=104857600
# The size of the socket send buffer for network communication
socket.send.buffer.bytes=102400
# Minimum in-sync replicas for the transaction log to consider a write successful
transaction.state.log.min.isr=1
# Replication factor for the transaction log
transaction.state.log.replication.factor=1
# If false, Kafka will not allow unclean leader elections
unclean.leader.election.enable=false
# Timeout in milliseconds for establishing a connection to Zookeeper
zookeeper.connection.timeout.ms=30000
# Zookeeper server to connect to for Kafka coordination
zookeeper.connect=localhost:49000
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
