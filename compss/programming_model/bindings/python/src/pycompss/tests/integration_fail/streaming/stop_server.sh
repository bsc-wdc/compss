#!/bin/bash

# Use start_server.sh to start streaming

# Required variables
streaming=$1
# same as streams.sh
zookeeper_log_dir="/tmp/zookeeper"
kafka_log_dir="/tmp/kafka-logs"

source "${COMPSS_HOME}/Runtime/scripts/system/runtime/streams.sh"

check_stream_env

clean_stream_env
