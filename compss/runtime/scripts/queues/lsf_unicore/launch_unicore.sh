#!/bin/bash

  # Get script parameters from environment
  COMPSS_HOME=$COMPSS_HOME
  LSB_DJOB_HOSTFILE=$LSB_DJOB_HOSTFILE

  # Get script parameters from unicore
  tasks_per_node=$Tasks_Per_Node
  tasks_in_master=$Tasks_In_Master
  worker_WD_type=$Worker_Working_Dir
  network=$Network
  library_path=$Library_Path
  cp=$ClassPath
  log_level=$Log_Level
  tracing=$Tracing
  graph=$Graph
  comm=$Comm

  # Leave COMPSs parameters in $*
  app=$PYCOMPSSFILE
  real_app_name=$PYCOMPSS_FILE_NAME
  arguments=$ARGUMENTS

  echo "---------------------------"
  echo "PyCOMPSs 4 UNICORE Launcher"
  echo "---------------------------"
  echo "COMPSS_HOME: " $COMPSS_HOME
  echo "LSB_DJOB_HOSTFILE: " $LSB_DJOB_HOSTFILE
  echo "tasks_per_node: " $tasks_per_node
  echo "tasks_in_master: " $tasks_in_master
  echo "worker_WD_type: " $worker_WD_type
  echo "network: " $network
  echo "library_path: " $library_path
  echo "cp: " $cp
  echo "log_level: " $log_level
  echo "tracing: " $tracing
  echo "graph: " $graph
  echo "comm: " $comm

  pwd=$(pwd)
  #echo "pwd: " $pwd

  bindingsPath=$COMPSS_HOME/Bindings/bindings-common/lib

  cp=$cp":"$ClassPath":"$pwd":"$bindingsPath":"$JAVA_HOME":"$JAVA_HOME/jre/lib/amd64/server/
  library_path=$library_path":"$bindingsPath":"$JAVA_HOME":"$JAVA_HOME/jre/lib/amd64/server/
  #export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME
  echo "extendedClasspath: " $cp
  echo "extendedLibraryPath: " $library_path
  #echo "ld_library_path: " $LD_LIBRARY_PATH

  #echo "app: " $app
  absoluteApp=$pwd"/"$app
  #echo "absoluteApp: " $absoluteApp
  #echo "realApp: " $real_app_name
  absoluteRealApp=$pwd"/"$real_app_name
  mv $absoluteApp $absoluteRealApp
  echo "absoluteRealApp: " $absoluteRealApp
  echo "arguments: " $arguments

  bindingsPath=$COMPSS_HOME/Bindings/bindings-common/lib

  echo "---------------------------"

  ${COMPSS_HOME}/Runtime/scripts/user/launch_compss \
       --

  #Set script variables
  export COMPSS_HOME=${COMPSS_HOME}
  export GAT_LOCATION=${COMPSS_HOME}/Dependencies/JAVA_GAT
  worker_install_dir=${COMPSS_HOME}/Runtime/scripts/system/
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
  sec=`/bin/date +%s`
  RESOURCES_FILE=${worker_working_dir}/resources_$sec.xml
  PROJECT_FILE=${worker_working_dir}/project_mn_$sec.xml

  #---------------------------------------------------------------------------------------
  # Begin creating the resources file and the project file
  /bin/cat > ${RESOURCES_FILE} << EOT
<?xml version="1.0" encoding="UTF-8"?>
<ResourceList>
  <Disk Name="gpfs">
    <MountPoint>/gpfs</MountPoint>
  </Disk>

EOT

  /bin/cat > ${PROJECT_FILE} << EOT
<?xml version="1.0" encoding="UTF-8"?>
<Project>

EOT

  # Get node list
  ASSIGNED_LIST=$(cat ${LSB_DJOB_HOSTFILE} | /usr/bin/sed -e 's/\.[^\ ]*//g')

  # ------------------------  
  declare new_AL=''

  for item in $ASSIGNED_LIST; do
    if [[ ! $new_AL =~ $item ]] ; then   # first time?
      new_AL="$new_AL $item"
    fi
  done
  ASSIGNED_LIST=${new_AL:1}                  # remove leading blank
  # ------------------------

  echo "Node list assigned is:"
  echo "${ASSIGNED_LIST}"
  # Remove the processors of the master node from the list
  MASTER_NODE=$(hostname)
  echo "Master will run in ${MASTER_NODE}"
  WORKER_LIST=$(echo ${ASSIGNED_LIST} | /usr/bin/sed -e "s/$MASTER_NODE//g")
  # To remove only once: WORKER_LIST=\`echo \$ASSIGNED_LIST | /usr/bin/sed -e "s/\$MASTER_NODE//"\`;
  echo "List of workers:"
  echo "${WORKER_LIST}"
 
  # Add worker slots on master if needed
  if [ ${tasks_in_master} -ne 0 ]; then
	ssh ${MASTER_NODE}${network} "/bin/mkdir -p ${worker_working_dir}"
	/bin/cat >> ${RESOURCES_FILE} << EOT
<Resource Name="${MASTER_NODE}${network}">
    <Capabilities>
      <Host>
        <TaskCount>0</TaskCount>
      </Host>
      <Processor>
        <Architecture>Intel</Architecture>
        <Speed>2.6</Speed>
        <CoreCount>${tasks_in_master}</CoreCount>
      </Processor>
      <OS>
        <OSType>Linux</OSType>
      </OS>
      <StorageElement>
        <Size>36</Size>
      </StorageElement>
      <Memory>
        <PhysicalSize>28</PhysicalSize>
      </Memory>
      <ApplicationSoftware>
        <Software>COMPSs</Software>
      </ApplicationSoftware>
      <FileSystem/>
      <NetworkAdaptor/>
    </Capabilities>
    <Requirements/>
    <Disks>
      <Disk Name="gpfs">
        <MountPoint>/gpfs</MountPoint>
      </Disk>
    </Disks>
    <Adaptors>
      <Adaptor name="es.bsc.compss.nio.master.NIOAdaptor">
         <MinPort>43001</MinPort>
         <MaxPort>43001</MaxPort>
      </Adaptor>
    </Adaptors>
  </Resource>

EOT
        /bin/cat >> ${PROJECT_FILE} << EOT
  <Worker Name="${MASTER_NODE}${network}">
    <InstallDir>${worker_install_dir}</InstallDir>
    <WorkingDir>${worker_working_dir}</WorkingDir>
    <LibraryPath>${library_path}</LibraryPath>
  </Worker>

EOT
  fi

  # Find the number of tasks to be executed on each node
  for node in ${WORKER_LIST}; do
	ssh $node${network} "/bin/mkdir -p ${worker_working_dir}"
	/bin/cat >> ${RESOURCES_FILE} << EOT
  <Resource Name="${node}${network}">
    <Capabilities>
      <Host>
        <TaskCount>0</TaskCount>
      </Host>
      <Processor>
        <Architecture>Intel</Architecture>
        <Speed>2.6</Speed>
        <CoreCount>${tasks_per_node}</CoreCount>
      </Processor>
      <OS>
        <OSType>Linux</OSType>
      </OS>
      <StorageElement>
        <Size>36</Size>
      </StorageElement>
      <Memory>
        <PhysicalSize>28</PhysicalSize>
      </Memory>
      <ApplicationSoftware>
        <Software>COMPSs</Software>
      </ApplicationSoftware>
      <FileSystem/>
      <NetworkAdaptor/>
    </Capabilities>
    <Requirements/>
    <Disks>
      <Disk Name="gpfs">
	<MountPoint>/gpfs</MountPoint>
      </Disk>
    </Disks>
    <Adaptors>
      <Adaptor name="es.bsc.compss.nio.master.NIOAdaptor">
         <MinPort>43001</MinPort>
         <MaxPort>43001</MaxPort>
      </Adaptor>
    </Adaptors>
  </Resource>

EOT
	/bin/cat >> ${PROJECT_FILE} << EOT
  <Worker Name="${node}${network}">
    <InstallDir>${worker_install_dir}</InstallDir>
    <WorkingDir>${worker_working_dir}</WorkingDir>
    <LibraryPath>${library_path}</LibraryPath>
  </Worker>

EOT
  done

  # Finish the resources file and the project file 
  /bin/cat >> ${RESOURCES_FILE} << EOT
</ResourceList>
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
  #MPI_CMD="mpirun -timestamp-output -n 1 -H $MASTER_NODE ${COMPSS_HOME}/Runtime/scripts/user/runcompss --project=${PROJECT_FILE} --resources=${RESOURCES_FILE} --uuid=$uuid $*"
  MPI_CMD="mpirun -timestamp-output -n 1 -H $MASTER_NODE ${COMPSS_HOME}/Runtime/scripts/user/runcompss --project=${PROJECT_FILE} --resources=${RESOURCES_FILE} --uuid=$uuid --lang=python --log_level=${log_level} --tracing=${tracing} --graph=$graph --library_path=$Library_Path --classpath=$cp --comm=$Comm $absoluteRealApp $arguments"
  if [ "${comm/NIO}" != "${comm}" ]; then
    # Adapt debug flag to worker script
    if [ "${log_level}" == "debug" ]; then
      debug="true"
    else
      debug="false"
    fi
    # Runtime will use NIO Adaptor. Starting MPI Workers
    if [ ${tasks_in_master} -ne 0 ]; then
      USED_WORKERS=${ASSIGNED_LIST}
    else
      USED_WORKERS=${WORKER_LIST}
    fi
    hostid=1
    for node in ${USED_WORKERS}; do
      MPI_CMD=$MPI_CMD" : -n 1 -H $node ${COMPSS_HOME}/Runtime/scripts/system/adaptors/nio/persistent_worker_starter.sh ${library_path} null ${cp} ${debug} ${worker_working_dir} ${tasks_per_node} 5 5 $node${network} 43001 43000 ${tracing} ${hostid} ${worker_install_dir} ${uuid}"
      hostid=$((hostid+1))
    done
  fi
  echo MPI_CMD=$MPI_CMD
  $MPI_CMD

  #---------------------------------------------------------------------------------------
  # Cleanup
  for node in ${WORKER_LIST}; do
	ssh $node${network} "/bin/rm -rf ${worker_working_dir}"
  done
  /bin/rm -rf ${PROJECT_FILE}
  /bin/rm -rf ${RESOURCES_FILE}

