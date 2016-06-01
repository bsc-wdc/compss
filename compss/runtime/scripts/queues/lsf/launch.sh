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
    
    if [ -z "$impi" ]; then
      # Load Extrae IMPI
      export EXTRAE_HOME=${IT_HOME}/Dependencies/extrae-impi/
    elif [ -z "$openmpi" ]; then
      # Load Extrae OpenMPI
      export EXTRAE_HOME=${IT_HOME}/Dependencies/extrae-openmpi/
    else 
      # Load sequential extrae
      export EXTRAE_HOME=${IT_HOME}/Dependencies/extrae/
    fi

    # Clean tmp file
    rm -f ${module_tmp}º
  }

  #---------------------------------------------------------------------------------------
  # MAIN
  #---------------------------------------------------------------------------------------
  #Get script parameters
  IT_HOME=$1
  LSB_DJOB_HOSTFILE=$2
  tasks_per_node=$3
  tasks_in_master=$4
  worker_WD_type=$5
  specific_log_dir=$6
  jvm_master_opts=$7
  jvm_workers_opts=$8
  network=$9
  master_port=${10}
  library_path=${11}
  cp=${12}
  log_level=${13}
  tracing=${14}
  comm=${15}
  storageName=${16}
  storageConf=${17}
  taskExecution=${18}

  #Leave COMPSs parameters in $*
  shift 18

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
  sec=$(/bin/date +%s)
  RESOURCES_FILE=${worker_working_dir}/resources_$sec.xml
  PROJECT_FILE=${worker_working_dir}/project_mn_$sec.xml


  #---------------------------------------------------------------------------------------
  # Begin creating the resources file and the project file
  /bin/cat > ${RESOURCES_FILE} << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ResourcesList>
    <SharedDisk Name="gpfs" />

EOT

  /bin/cat > ${PROJECT_FILE} << EOT
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
	  for node in ${ASSIGNED_LIST}
	  do
		if [ $i -gt 1 ]
		then
		      WORKER_LIST=${WORKER_LIST}$node$space
		fi
		let i=i+1
	  done
	  WORKER_LIST=${WORKER_LIST%?}
  fi

  echo "List of workers:"
  echo "${WORKER_LIST}"
 
  # Add worker slots on master if needed
  if [ ${tasks_in_master} -ne 0 ]; then
	ssh ${MASTER_NODE}${network} "/bin/mkdir -p ${worker_working_dir}"
	/bin/cat >> ${RESOURCES_FILE} << EOT
    <ComputeNode Name="${MASTER_NODE}${network}">
        <Processor Name="MainProcessor">
            <ComputingUnits>${tasks_in_master}</ComputingUnits>
	    <Architecture>Intel</Architecture>
	    <Speed>2.6</Speed>
        </Processor>
        <OperatingSystem>
	    <Type>Linux</Type>
	    <Distribution>SMP</Distribution>
            <Version>3.0.101-0.35-default</Version>
	</OperatingSystem>
        <Memory>
            <Size>28</Size>
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

        /bin/cat >> ${PROJECT_FILE} << EOT
    <ComputeNode Name="${MASTER_NODE}${network}">
        <InstallDir>${worker_install_dir}</InstallDir>
        <WorkingDir>${worker_working_dir}</WorkingDir>
        <Application>
            <LibraryPath>${library_path}</LibraryPath>
        </Application>
    </ComputeNode>

EOT
  fi

  # Find the number of tasks to be executed on each node
  for node in ${WORKER_LIST}; do
	ssh $node${network} "/bin/mkdir -p ${worker_working_dir}"
	/bin/cat >> ${RESOURCES_FILE} << EOT
    <ComputeNode Name="${node}${network}">
        <Processor Name="MainProcessor">
            <ComputingUnits>${tasks_per_node}</ComputingUnits>
            <Architecture>Intel</Architecture>
            <Speed>2.6</Speed>
        </Processor>
        <OperatingSystem>
            <Type>Linux</Type>
            <Distribution>SMP</Distribution>
            <Version>3.0.101-0.35-default</Version>
        </OperatingSystem>
        <Memory>
            <Size>28</Size>
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

	/bin/cat >> ${PROJECT_FILE} << EOT
    <ComputeNode Name="${node}${network}">
        <InstallDir>${worker_install_dir}</InstallDir>
        <WorkingDir>${worker_working_dir}</WorkingDir>
        <Application>
            <LibraryPath>${library_path}</LibraryPath>
        </Application>
    </ComputeNode>

EOT
  done

  # Finish the resources file and the project file 
  /bin/cat >> ${RESOURCES_FILE} << EOT
</ResourcesList>
EOT

  /bin/cat >> ${PROJECT_FILE} << EOT
</Project>
EOT

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
    # Get workers list
    if [ ${tasks_in_master} -ne 0 ]; then
      USED_WORKERS=${ASSIGNED_LIST}
    else
      USED_WORKERS=${WORKER_LIST}
    fi
    # Start workers' processes
    hostid=1
    jvm_workers_opts_str=$(echo "${jvm_workers_opts}" | tr "," " ")
    jvm_workers_opts_size=$(echo "${jvm_workers_opts_str}" | wc -w)
    for node in ${USED_WORKERS}; do
      sandbox_worker_working_dir=${worker_working_dir}/${uuid}/${node}${network}
      WCMD="blaunch $node ${IT_HOME}/Runtime/scripts/system/adaptors/nio/persistent_worker_starter.sh ${library_path} null ${cp} ${jvm_workers_opts_size} ${jvm_workers_opts_str} ${debug} ${tasks_per_node} 5 5 $node${network} 43001 ${master_port} ${uuid} ${sandbox_worker_working_dir} ${worker_install_dir} ${w_tracing} ${hostid} ${storageConf} ${taskExecution}"
      echo "CMD Worker $hostid launcher: $WCMD"
      $WCMD&
      hostid=$((hostid+1))
    done
  fi

  # Launch master
  MCMD="blaunch $MASTER_NODE ${IT_HOME}/Runtime/scripts/user/runcompss --master_port=${master_port} --project=${PROJECT_FILE} --resources=${RESOURCES_FILE} --storage_conf=${storageConf} --task_execution=${taskExecution} --uuid=${uuid} --jvm_master_opts="${jvm_master_opts}" --jvm_workers_opts="${jvm_workers_opts}" --specific_log_dir=${specific_log_dir} $*"
  echo "CMD Master: $MCMD"
  $MCMD&

  # Wait for Master and Workers to finish
  echo "Waiting for application completition"
  wait

  #---------------------------------------------------------------------------------------
  # Cleanup
  echo "Cleanup TMP files"
  for node in ${WORKER_LIST}; do
	ssh $node${network} "rm -rf ${worker_working_dir}"
  done
  rm -rf ${PROJECT_FILE}
  rm -rf ${RESOURCES_FILE}

