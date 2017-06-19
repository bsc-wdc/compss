#!/bin/bash

  usage() {
    echo "Usage: "
    echo "    $0 <workerUser> <workerIP> <resourcesFile>"
    echo " "
  }

  execute_remote_command() {
    local targetUser=$1
    local targetIP=$2
    local cmd=$3

    ev=0
    if [ "$targetUser" == "" ]; then
      result=$(ssh ${targetIP} $cmd)
      ev=$?
    else 
      result=$(ssh ${targetUser}@${targetIP} $cmd)
      ev=$?
    fi

    if [ $ev -ne 0 ]; then 
      echo "[ERROR] Cannot execute $cmd remotely on $targetIP with user $targetUser"
      exit 1
    fi
  }

  write_to_file() {
    local file=$1
    local workerName=$2
    local cus=$3
    local arch=$4
    local speed=$5
    local memory=$6
    local disk=$7
    local osType=$8
    local osDistr=$9
    local osVersion=${10}

    cat >> ${file} << EOT
    
    <ComputeNode Name="${workerName}">
        <Processor Name="P1">
            <ComputingUnits>${cus}</ComputingUnits>
            <Architecture>${arch}</Architecture>
            <Speed>${speed}</Speed>
        </Processor>
        <Adaptors>
            <Adaptor Name="integratedtoolkit.nio.master.NIOAdaptor">
                <SubmissionSystem>
                    <Interactive/>
                </SubmissionSystem>
                <Ports>
                    <MinPort>43001</MinPort>
                    <MaxPort>43002</MaxPort>
                </Ports>
            </Adaptor>
            <Adaptor Name="integratedtoolkit.gat.master.GATAdaptor">
                <SubmissionSystem>
                    <Batch>
                        <Queue>sequential</Queue>
                        <Queue>training</Queue>
                    </Batch>
                    <Interactive/>
                </SubmissionSystem>
                <BrokerAdaptor>sshtrilead</BrokerAdaptor>
            </Adaptor>
        </Adaptors>
        <Memory>
            <Size>${memory}</Size>
            <!-- <Type>Non-volatile</Type> -->
        </Memory>
        <Storage>
            <Size>${disk}</Size>
        </Storage>
        <OperatingSystem>
            <Type>${osType}</Type>
            <Distribution>${osDistr}</Distribution>
            <Version>${osVersion}</Version>
        </OperatingSystem>
        <Software>
            <Application>Java</Application>
            <Application>Python</Application>
        </Software>
    </ComputeNode>

EOT
  }


  ###########################################################
  # MAIN
  ###########################################################

  # Arguments
  # echo "[ADD_WORKER] [DEBUG] Parsing arguments"
  if [ $# -ne 3 ]; then
    echo "[ADD_WORKER] [ERROR] Invalid number of arguments"
    usage
    exit 1
  fi
  workerUser=$1
  workerIP=$2
  resourcesFile=$3

  # Retrieving worker information
  echo "[ADD_WORKER] [DEBUG] Retrieving ${workerIP} information"

  cmd="lscpu | grep 'CPU(s):' | awk {' print \$NF '}"
  execute_remote_command "${workerUser}" "${workerIP}" "$cmd"
  computingUnits="$result"

  cmd="lscpu | grep 'Arquitectura:' | awk {' print \$NF '}"
  execute_remote_command "${workerUser}" "${workerIP}" "$cmd"
  architecture="$result"

  cmd="lscpu | grep 'CPU MHz:' | awk {' print \$NF '}"
  execute_remote_command "${workerUser}" "${workerIP}" "$cmd"
  cpuSpeed="$result"

  cmd="cat /proc/meminfo | grep 'MemTotal' | awk {' print \$(NF-1) '}"
  execute_remote_command "${workerUser}" "${workerIP}" "$cmd"
  memSize="$result"

  cmd="df -Ph / | awk {' print \$2 '} | tail -n 1 | tr 'G' ' ' | tr 'M' ' ' | awk {' print \$1 '}"
  execute_remote_command "${workerUser}" "${workerIP}" "$cmd"
  diskSize="$result"

  operatingSystemType="Linux"

  cmd="lsb_release -a | grep Description | awk '{for (i=2; i<NF; i++) printf \$i \" \"; print \$NF}'"
  execute_remote_command "${workerUser}" "${workerIP}" "$cmd"
  operatingSystemDistr="$result"

  cmd="lsb_release -a | grep Release | awk {' print \$NF '}"
  execute_remote_command "${workerUser}" "${workerIP}" "$cmd"
  operatingSystemVersion="$result"

  # Appending to file
  echo "[ADD_WORKER] [DEBUG] Adding ${workerIP} to ${resourcesFile} with: "
  echo "[ADD_WORKER] [DEBUG]    - Computing Units:	$computingUnits"
  echo "[ADD_WORKER] [DEBUG]    - Architecture:     	$architecture"
  echo "[ADD_WORKER] [DEBUG]    - CPU Speed:		$cpuSpeed"
  echo "[ADD_WORKER] [DEBUG]    - Memory Size:		$memSize"
  echo "[ADD_WORKER] [DEBUG]    - Disk Size:		$diskSize"
  echo "[ADD_WORKER] [DEBUG]    - OS Type:		$operatingSystemType"
  echo "[ADD_WORKER] [DEBUG]    - OS Distribution:	$operatingSystemDistr"
  echo "[ADD_WORKER] [DEBUG]    - OS Version:		$operatingSystemVersion"

  write_to_file "${resourcesFile}" "$workerIP" "$computingUnits" "$architecture" "$cpuSpeed" "$memSize" "$diskSize" "$operatingSystemType" "$operatingSystemDistr" "$operatingSystemVersion"

  # DONE
  echo "[ADD_WORKER] [INFO] DONE"
  exit 0

