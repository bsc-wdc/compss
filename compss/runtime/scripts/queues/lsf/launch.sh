#!/bin/bash

  #---------------------------------------------------------------------------------------
  # HELPER FUNCTIONS
  #---------------------------------------------------------------------------------------
  load_tracing_env() {
    local module_tmp=$(mktemp)
    module list 2> ${module_tmp}

    # Look for openmpi / impi / none
    impi=$(cat ${module_tmp} | grep -i "impi")
    openmpi=$(cat ${module_tmp} | grep -i "openmpi")
    
    if [ ! -z "$impi" ]; then
      # Load Extrae IMPI
      export EXTRAE_HOME=${IT_HOME}/Dependencies/extrae-impi/
    elif [ ! -z "$openmpi" ]; then
      # Load Extrae OpenMPI
      export EXTRAE_HOME=${IT_HOME}/Dependencies/extrae-openmpi/
    else 
      # Load sequential extrae
      export EXTRAE_HOME=${IT_HOME}/Dependencies/extrae/
    fi

    # Clean tmp file
    rm -f ${module_tmp}
  }

  insert_xml_headers() {
    cat > ${RESOURCES_FILE} << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ResourcesList>
    <SharedDisk Name="gpfs" />

EOT

    cat > ${PROJECT_FILE} << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Project>
    <MasterNode>
        <SharedDisks>
            <AttachedDisk Name="gpfs">
                <MountPoint>/gpfs/</MountPoint>
            </AttachedDisk>
        </SharedDisks>
    </MasterNode>

EOT

  }

  insert_xml_footers() {
    cat >> ${RESOURCES_FILE} << EOT
</ResourcesList>
EOT

    cat >> ${PROJECT_FILE} << EOT
</Project>
EOT

  }

  add_compute_node() {
    local nodeName=$1
    local cus=$2
    local memory=$3

    ssh ${MASTER_NODE}${network} "/bin/mkdir -p ${worker_working_dir}"

    cat >> ${RESOURCES_FILE} << EOT
    <ComputeNode Name="${nodeName}">
        <Processor Name="MainProcessor">
            <ComputingUnits>${cus}</ComputingUnits>
            <Architecture>Intel</Architecture>
            <Speed>2.6</Speed>
        </Processor>
        <OperatingSystem>
            <Type>Linux</Type>
            <Distribution>SMP</Distribution>
            <Version>3.0.101-0.35-default</Version>
        </OperatingSystem>
        <Memory>
            <Size>${memory}</Size>
        </Memory>
        <Software>
            <Application>JAVA</Application>
            <Application>PYTHON</Application>
            <Application>EXTRAE</Application>
            <Application>COMPSS</Application>
        </Software>
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
                    <Interactive/>
                </SubmissionSystem>
                <BrokerAdaptor>sshtrilead</BrokerAdaptor>
            </Adaptor>
        </Adaptors>
        <SharedDisks>
            <AttachedDisk Name="gpfs">
                <MountPoint>/gpfs/</MountPoint>
            </AttachedDisk>
        </SharedDisks>
    </ComputeNode>

EOT

    cat >> ${PROJECT_FILE} << EOT
    <ComputeNode Name="${nodeName}">
        <InstallDir>${worker_install_dir}</InstallDir>
        <WorkingDir>${worker_working_dir}</WorkingDir>
        <Application>
            <LibraryPath>${library_path}</LibraryPath>
        </Application>
    </ComputeNode>

EOT
  }

  worker_cmd() {
    # WARNING: SETS GLOBAL SCRIPT VARIABLE WCMD
    local nodeId=$1
    local nodeName=$2
    local jvm_opts_size=$3
    local jvm_opts_str=$4
    local cus=$5

    local sandbox_worker_working_dir=${worker_working_dir}/${uuid}/${nodeName}
    local maxSend=5
    local maxReceive=5
    local worker_port=43001

    WCMD="${IT_HOME}/Runtime/scripts/system/adaptors/nio/persistent_worker_starter.sh \
              ${library_path} \
              null \
              ${cp} \
              ${jvm_opts_size} \
              ${jvm_opts_str} \
              ${debug} \
              ${cus} \
              ${maxSend} \
              ${maxReceive} \
              ${nodeName} \
              ${worker_port} \
              ${master_port} \
              ${uuid} \
              ${lang} \
              ${sandbox_worker_working_dir} \
              ${worker_install_dir} \
              null \
              ${library_path} \
              ${cp} \
              ${pythonpath} \
              ${w_tracing} \
              ${nodeId} \
              ${storageConf} \
              ${taskExecution}"
  }

  master_cmd() {
    # WARNING: SETS GLOBAL SCRIPT VARIABLE MCMD
    MCMD="${IT_HOME}/Runtime/scripts/user/runcompss \
              --master_port=${master_port} \
              --project=${PROJECT_FILE} \
              --resources=${RESOURCES_FILE} \
              --storage_conf=${storageConf} \
              --task_execution=${taskExecution} \
              --uuid=${uuid} \
              --jvm_master_opts="${jvm_master_opts}" \
              --jvm_workers_opts="${jvm_workers_opts}" \
              --specific_log_dir=${specific_log_dir}"
  }  


  #---------------------------------------------------------------------------------------
  # MAIN
  #---------------------------------------------------------------------------------------
  #Get script parameters
  IT_HOME=$1
  LSB_DJOB_HOSTFILE=$2
  tasks_per_node=$3
  worker_in_master_tasks=$4
  worker_in_master_memory=$5
  worker_WD_type=$6
  specific_log_dir=$7
  jvm_master_opts=$8
  jvm_workers_opts=$9
  jvm_worker_in_master_opts=${10}
  network=${11}
  node_memory=${12}
  master_port=${13}
  library_path=${14}
  cp=${15}
  pythonpath=${16}
  lang=${17}
  log_level=${18}
  tracing=${19}
  comm=${20}
  storageName=${21}
  storageConf=${22}
  taskExecution=${23}

  #Leave COMPSs parameters in $*
  shift 23

  #Set script variables
  export IT_HOME=${IT_HOME}
  export GAT_LOCATION=${IT_HOME}/Dependencies/JAVA_GAT
  worker_install_dir=${IT_HOME}
  if [ "${worker_WD_type}" == "gpfs" ]; then
     worker_working_dir=$(mktemp -d -p /gpfs/${HOME})
  elif [ "${worker_WD_type}" == "scratch" ]; then
     worker_working_dir=$TMPDIR
  else 
     # The working dir is a custom absolute path, create tmp
     worker_working_dir=$(mktemp -d -p ${worker_WD_type})
  fi
  if [ "${network}" == "ethernet" ]; then
    network=""
  elif [ "${network}" == "infiniband" ]; then
    network="-ib0"
  elif [ "${network}" == "data" ]; then
    network="-data"
  fi
  if [ "${node_memory}" == "disabled" ]; then
    # Default value
    node_memory=28
  else
    # Change from MB to GB
    node_memory=$(( node_memory / 1024 - 4))
  fi

  sec=$(date +%s)
  RESOURCES_FILE=${worker_working_dir}/resources_$sec.xml
  PROJECT_FILE=${worker_working_dir}/project_mn_$sec.xml


  #---------------------------------------------------------------------------------------
  # Begin creating the resources file and the project file
  insert_xml_headers

  # Get node list
  ASSIGNED_LIST=$(cat ${LSB_DJOB_HOSTFILE} | /usr/bin/sed -e 's/\.[^\ ]*//g')
  echo "Node list assigned is:"
  echo "${ASSIGNED_LIST}"
  # Remove the processors of the master node from the list
  MASTER_NODE=$(hostname)
  echo "Master will run in ${MASTER_NODE}"

  if [ "${storageName}" != "dataclay" ]; then
    WORKER_LIST=$(echo ${ASSIGNED_LIST} | /usr/bin/sed -e "s/$MASTER_NODE//g")
    # To remove only once: WORKER_LIST=\`echo \$ASSIGNED_LIST | /usr/bin/sed -e "s/\$MASTER_NODE//"\`;
  else 
    # Skip node assigned to COMPSs master and node assigned to DataClay Logic Module
    i=0
    space=" "
    for node in ${ASSIGNED_LIST}; do
      if [ $i -gt 1 ]; then
        WORKER_LIST=${WORKER_LIST}$node$space
      fi
      let i=i+1
    done
    WORKER_LIST=${WORKER_LIST%?}
  fi

  echo "List of workers:"
  echo "${WORKER_LIST}"
 
  # Add worker slots on master if needed
  if [ ${worker_in_master_tasks} -ne 0 ]; then
    add_compute_node "${MASTER_NODE}${network}" ${worker_in_master_tasks} ${worker_in_master_memory}        
  fi

  # Find the number of tasks to be executed on each node
  for node in ${WORKER_LIST}; do
    add_compute_node "$node${network}" ${tasks_per_node} ${node_memory}
  done

  # Finish the resources file and the project file 
  insert_xml_footers

  echo "Generation of resources and project file finished"
  echo "Project.xml:   ${PROJECT_FILE}"
  echo "Resources.xml: ${RESOURCES_FILE}"

  #---------------------------------------------------------------------------------------
  # Generate a UUID for workers and runcompss
  uuid=$(cat /proc/sys/kernel/random/uuid)

  #---------------------------------------------------------------------------------------
  # Launch the application with COMPSs
  echo "Launching application"

  # Launch workers separately if they are persistent
  if [ "${comm/NIO}" != "${comm}" ]; then
    # Adapting tracing flag to worker tracing level
    if [ -z "$tracing" ]; then
       w_tracing=0
    elif [ $tracing == "false" ]; then
       w_tracing=0
    elif [ $tracing == "basic" ] || [ $tracing == "true" ]; then
       w_tracing=1
       load_tracing_env
    elif [ $tracing == "advanced" ]; then
       w_tracing=2
       load_tracing_env
    fi

    # Adapt debug flag to worker script
    if [ "${log_level}" == "debug" ]; then
      debug="true"
    else
      debug="false"
    fi

    # Start workers' processes
    hostid=1
    if [ ${worker_in_master_tasks} -ne 0 ]; then
      # Worker in master node
      jvm_worker_in_master_opts_str=$(echo "${jvm_worker_in_master_opts}" | tr "," " ")
      jvm_worker_in_master_opts_size=$(echo "${jvm_worker_in_master_opts_str}" | wc -w)
      worker_cmd $hostid "${MASTER_NODE}${network}" ${jvm_worker_in_master_opts_size} "${jvm_worker_in_master_opts_str}" ${worker_in_master_tasks}
      WCMD="blaunch ${MASTER_NODE} ${WCMD}"
      echo "CMD Worker $hostid launcher: $WCMD"
      $WCMD&
      hostid=$((hostid+1))
    fi

    jvm_workers_opts_str=$(echo "${jvm_workers_opts}" | tr "," " ")
    jvm_workers_opts_size=$(echo "${jvm_workers_opts_str}" | wc -w)
    for node in ${WORKER_LIST}; do
      worker_cmd $hostid "$node${network}" ${jvm_workers_opts_size} "${jvm_workers_opts_str}" ${tasks_per_node}
      WCMD="blaunch ${node} ${WCMD}"
      echo "CMD Worker $hostid launcher: $WCMD"
      $WCMD&
      hostid=$((hostid+1))
    done
  fi

  # Launch master
  master_cmd
  MCMD="blaunch $MASTER_NODE ${MCMD} $*"
  echo "CMD Master: $MCMD"
  $MCMD&

  # Wait for Master and Workers to finish
  echo "Waiting for application completion"
  wait

  #---------------------------------------------------------------------------------------
  # Cleanup
  echo "Cleanup TMP files"
  for node in ${WORKER_LIST}; do
    ssh $node${network} "rm -rf ${worker_working_dir}"
  done
  rm -f ${PROJECT_FILE}
  rm -f ${RESOURCES_FILE}

