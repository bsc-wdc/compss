#!/bin/bash

# Use start_server.sh to start streaming

# Required variables
streaming=$1
streaming_master_name=$2
streaming_master_port=$3

source "${COMPSS_HOME}/Runtime/scripts/system/runtime/streams.sh"

check_stream_env

check_stream_setup

generate_stream_config_files

start_stream_backends