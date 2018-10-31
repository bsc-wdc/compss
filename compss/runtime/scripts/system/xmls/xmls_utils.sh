#!/bin/bash


#---------------------------------------------------
# MAIN FUNCTIONS DECLARATION
#---------------------------------------------------

###############################################
# Creates XML Files
###############################################
create_xml_files() {
  # Resources.xml and project.xml filenames
  local sec
  sec=$(date +%s)

  # Define the destination xml folder
  RESOURCES_FILE=${worker_working_dir}/resources_$sec.xml
  PROJECT_FILE=${worker_working_dir}/project_$sec.xml

  # Begin creating the resources file and the project file
  insert_xml_headers

  # Log Master Node
  echo "Master will run in ${master_node}"

  # Log List of Workers
  echo "List of workers: ${worker_nodes}"

  # Add worker slots on master if needed
  if [ ! -z "${NODE_NAME_XML}" ]; then
    master_node=$(${NODE_NAME_XML} "${master_node}" "${network}")
  fi

  if [ "${worker_in_master_cpus}" -ne 0 ]; then
    # add_compute_node name cpus gpus lot memory
    add_compute_node "${master_node}" "${worker_in_master_cpus}" "${gpus_per_node}" "${fpgas_per_node}" "${max_tasks_per_node}" "${worker_in_master_memory}"
  fi

  # Find the number of tasks to be executed on each node
  for node in ${worker_nodes}; do
      if [ ! -z "${NODE_NAME_XML}" ]; then
        node=$(${NODE_NAME_XML} "${node}" "${network}")
      fi
    # add_compute_node name cpus gpus lot memory
    add_compute_node "${node}" "${cpus_per_node}" "${gpus_per_node}" "${fpgas_per_node}" "${max_tasks_per_node}" "${node_memory}"
  done
  if [ ! -z "${elasticity}" ]; then
    insert_xml_elasticity "${elasticity}" "${cpus_per_node}" "${gpus_per_node}" "${fpgas_per_node}" "${max_tasks_per_node}" "${node_memory}"
  fi
  # Finish the resources file and the project file
  insert_xml_footers

  echo "Generation of resources and project file finished"
  echo "Project.xml:   ${PROJECT_FILE}"
  echo "Resources.xml: ${RESOURCES_FILE}"
}

init_het_xml_files() {
  # Resources.xml and project.xml filenames
  
  suffix=$1

  # Define the destination xml folder
  RESOURCES_FILE=${worker_working_dir}/resources_$suffix.xml
  PROJECT_FILE=${worker_working_dir}/project_$suffix.xml

  echo "Generating resources and project files"
  echo "Project.xml:   ${PROJECT_FILE}"
  echo "Resources.xml: ${RESOURCES_FILE}"
  
  # Begin creating the resources file and the project file
  insert_xml_headers

  echo "List of workers: ${worker_nodes}"

  # Find the number of tasks to be executed on each node
  for node in ${worker_nodes}; do
      if [ ! -z "${NODE_NAME_XML}" ]; then
        node=$(${NODE_NAME_XML} "${node}" "${network}")
      fi
    # add_compute_node name cpus gpus lot memory
    add_compute_node "${node}" "${cpus_per_node}" "${gpus_per_node}" "${fpgas_per_node}" "${max_tasks_per_node}" "${node_memory}"
  done
  if [ ! -z "${elasticity}" ]; then
    echo " WARNING: Heterogeneity with elasticity not yet supported !"
  fi
}

add_het_xml_files() {
  # Resources.xml and project.xml filenames

  suffix=$1

  # Define the destination xml folder
  RESOURCES_FILE=${worker_working_dir}/resources_$suffix.xml
  PROJECT_FILE=${worker_working_dir}/project_$suffix.xml

  echo "Continue generating resources and project files"
  echo "Project.xml:   ${PROJECT_FILE}"
  echo "Resources.xml: ${RESOURCES_FILE}"
  echo "List of workers: ${worker_nodes}"

  # Find the number of tasks to be executed on each node
  for node in ${worker_nodes}; do
      if [ ! -z "${NODE_NAME_XML}" ]; then
        node=$(${NODE_NAME_XML} "${node}" "${network}")
      fi
    # add_compute_node name cpus gpus lot memory
    add_compute_node "${node}" "${cpus_per_node}" "${gpus_per_node}" "${fpgas_per_node}" "${max_tasks_per_node}" "${node_memory}"
  done
}

fini_het_xml_files() {
  # Resources.xml and project.xml filenames
  suffix=$1

  # Define the destination xml folder
  RESOURCES_FILE=${worker_working_dir}/resources_$suffix.xml
  PROJECT_FILE=${worker_working_dir}/project_$suffix.xml

  echo "Finishing generation of resources and project files"
  echo "Project.xml:   ${PROJECT_FILE}"
  echo "Resources.xml: ${RESOURCES_FILE}"
  
    # Log Master Node
  echo "Master will run in ${master_node}"

  # Add worker slots on master if needed
  if [ ! -z "${NODE_NAME_XML}" ]; then
    master_node=$(${NODE_NAME_XML} "${master_node}" "${network}")
  fi

  if [ "${worker_in_master_cpus}" -ne 0 ]; then
    # add_compute_node name cpus gpus lot memory
    add_compute_node "${master_node}" "${worker_in_master_cpus}" "${gpus_per_node}" "${fpgas_per_node}" "${max_tasks_per_node}" "${worker_in_master_memory}"
  fi
  # Finish the resources file and the project file
  insert_xml_footers

  echo "Generation of resources and project file finished"
}

#---------------------------------------------------------------------------------------
# XML SPECIFIC FUNCTIONS
#---------------------------------------------------------------------------------------

###############################################
# Insert XML Headers
###############################################
insert_xml_headers() {
  if [ "${GPFS2_PREFIX}" != "" ] && [ "${GPFS2_PREFIX}" != "${GPFS_PREFIX}" ]; then
    cat > "${RESOURCES_FILE}" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ResourcesList>
  <SharedDisk Name="gpfs" />
  <SharedDisk Name="gpfs2" />

EOT
  elif [ "${GPFS_PREFIX}" != "" ]; then
    cat > "${RESOURCES_FILE}" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ResourcesList>
  <SharedDisk Name="gpfs" />

EOT
  else
    cat > "${RESOURCES_FILE}" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<ResourcesList>
EOT
  fi

if [ "${GPFS_PREFIX}" != "" ]; then
  cat > "${PROJECT_FILE}" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Project>
  <MasterNode>
      <SharedDisks>
          <AttachedDisk Name="gpfs">
              <MountPoint>${GPFS_PREFIX}</MountPoint>
          </AttachedDisk>
EOT
  if [ "${GPFS2_PREFIX}" != "" ] && [ "${GPFS2_PREFIX}" != "${GPFS_PREFIX}" ]; then
    cat >> "${PROJECT_FILE}" << EOT
          <AttachedDisk Name="gpfs2">
              <MountPoint>${GPFS2_PREFIX}</MountPoint>
          </AttachedDisk>
EOT
  fi

cat >> "${PROJECT_FILE}" << EOT
      </SharedDisks>
  </MasterNode>

EOT

else
    cat > "${PROJECT_FILE}" << EOT
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Project>
  <MasterNode></MasterNode>
EOT

fi
}

###############################################
# Insert XML Footers
###############################################
insert_xml_footers() {
  cat >> "${RESOURCES_FILE}" << EOT
</ResourcesList>
EOT

  cat >> "${PROJECT_FILE}" << EOT
</Project>
EOT
}


###############################################
# Insert XML Elasticity
###############################################
insert_xml_elasticity() {
  local max_vms=$1
  local cus=$2
  local gpus=$3
  local fpgas=$4
  local lot=$5
  local memory=$6
  local jvm_workers_opts_str
  local jvm_workers_opts_size
  jvm_workers_opts_str=$(echo "${jvm_workers_opts}" | tr "," " ")
  jvm_workers_opts_size=$(echo "${jvm_workers_opts_str}" | wc -w)
  fpga_reprogram_str=$(echo "${fpga_prog}" | tr "," " ")
  fpga_reprogram_size=$(echo "${fpga_prog}" | wc -w)

  cat >> "${RESOURCES_FILE}" << EOT
   <CloudProvider Name="SLURM-Cluster">
        <Endpoint>
            <Server></Server>
            <ConnectorJar>slurm-conn.jar</ConnectorJar>
            <ConnectorClass>es.bsc.conn.slurm.SLURMConnector</ConnectorClass>
        </Endpoint>
        <Images>
EOT
  if [ -z "${container_image}" ]; then
	cat >> "${RESOURCES_FILE}" << EOT
            <Image Name="None">
EOT
  else
       cat >> "${RESOURCES_FILE}" << EOT
            <Image Name="${container_image}">
EOT
  fi
  cat >> "${RESOURCES_FILE}" << EOT
                <CreationTime>10</CreationTime>
      		<OperatingSystem>
          		<Type>Linux</Type>
          		<Distribution>SMP</Distribution>
          		<Version>3.0.101-0.35-default</Version>
      		</OperatingSystem>
      		<Software>
          		<Application>JAVA</Application>
          		<Application>PYTHON</Application>
          		<Application>EXTRAE</Application>
          		<Application>COMPSS</Application>
      		</Software>
      		<Adaptors>
          		<Adaptor Name="es.bsc.compss.nio.master.NIOAdaptor">
              			<SubmissionSystem>
                  			<Interactive/>
              			</SubmissionSystem>
              			<Ports>
                  			<MinPort>43001</MinPort>
                  			<MaxPort>43002</MaxPort>
                  			<RemoteExecutionCommand>${REMOTE_EXECUTOR}</RemoteExecutionCommand>
              			</Ports>
          		</Adaptor>
      		</Adaptors>
EOT

  if [ "${GPFS_PREFIX}" != "" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
      		<SharedDisks>
          		<AttachedDisk Name="gpfs">
              			<MountPoint>${GPFS_PREFIX}</MountPoint>
          		</AttachedDisk>
EOT
    if [ "${GPFS2_PREFIX}" != "" ] && [ "${GPFS2_PREFIX}" != "${GPFS_PREFIX}" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
          		<AttachedDisk Name="gpfs2">
              			<MountPoint>${GPFS2_PREFIX}</MountPoint>
          		</AttachedDisk>
EOT
    fi

    cat >> "${RESOURCES_FILE}" << EOT
      		</SharedDisks>
EOT
  fi

  cat >> "${RESOURCES_FILE}" << EOT
            </Image>
        </Images>
        <InstanceTypes>
            <InstanceType Name="default">
		<Processor Name="MainProcessor">
          		<ComputingUnits>${cus}</ComputingUnits>
          		<Architecture>Intel</Architecture>
          		<Speed>2.6</Speed>
      		</Processor>
EOT
  if [ "${gpus}" != "$DEFAULT_GPUS_PER_NODE" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
        	<Processor Name="GPU">
            		<Type>GPU</Type>
            		<ComputingUnits>${gpus}</ComputingUnits>
            		<Architecture>k80</Architecture>
            		<Speed>2.6</Speed>
        	</Processor>
EOT
  fi
  if [ "${fpgas}" != "$DEFAULT_FPGAS_PER_NODE" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
                <Processor Name="FPGA">
                        <Type>FPGA</Type>
                        <ComputingUnits>${fpgas}</ComputingUnits>
                        <Architecture>altera</Architecture>
                        <Speed>1.0</Speed>
                </Processor>
EOT
  fi
  cat >> "${RESOURCES_FILE}" << EOT
		<Memory>
          		<Size>${memory}</Size>
      		</Memory>
                <Price>
                    <TimeUnit>1</TimeUnit>
                    <PricePerUnit>0.085</PricePerUnit>
                </Price>
            </InstanceType>
        </InstanceTypes>
    </CloudProvider>

EOT

  cat >> "${PROJECT_FILE}" << EOT
  <Cloud>
        <InitialVMs>0</InitialVMs>
        <MinimumVMs>0</MinimumVMs>
        <MaximumVMs>${max_vms}</MaximumVMs>
        <CloudProvider Name="SLURM-Cluster">
		<LimitOfVMs>${max_vms}</LimitOfVMs>
            		<Properties>
                		<Property>
                    			<Name>master_name</Name>
                    			<Value>${master_node}</Value>
                		</Property>
EOT
  if [ -z "${container_image}" ]; then
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                    			<Name>slurm_over_ssh</Name>
                    			<Value>false</Value>
                		</Property>
EOT
  else
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                    			<Name>slurm_over_ssh</Name>
                    			<Value>true</Value>
                		</Property>
EOT
  fi
  if [ -n "${queue}" ]; then
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                                        <Name>queue</Name>
                                        <Value>${queue}</Value>
                                </Property>
EOT
  fi
  if [ -n "${reservation}" ]; then
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                                        <Name>reservation</Name>
                                        <Value>${reservation}</Value>
                                </Property>
EOT
  fi
  if [ -n "${qos}" ]; then
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                                        <Name>qos</Name>
                                        <Value>${qos}</Value>
                                </Property>
EOT
  fi
  if [ -n "${constraints}" ]; then
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                                        <Name>constraints</Name>
                                        <Value>${constraints}</Value>
                                </Property>
EOT
  fi
  if [ -n "${container_opts}" ]; then
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                                        <Name>container_opts</Name>
                                        <Value>${container_opts}</Value>
                                </Property>
EOT
  fi
  if [ -n "${cpu_affinity}" ]; then
          cat >> "${PROJECT_FILE}" << EOT
                                <Property>
                                        <Name>cpu_affinity</Name>
                                        <Value>${cpu_affinity}</Value>
                                </Property>
EOT
  fi

  cat >> "${PROJECT_FILE}" << EOT
				<Property>
                                        <Name>master_port</Name>
                                        <Value>${master_port}</Value>
                                </Property>
				<Property>
                                        <Name>worker_debug</Name>
                                        <Value>${debug}</Value>
                                </Property>
				<Property>
                                        <Name>jvm_opts_size</Name>
                                        <Value>${jvm_workers_opts_size}</Value>
                                </Property>
				<Property>
                                        <Name>jvm_opts_str</Name>
                                        <Value>${jvm_workers_opts_str}</Value>
                                </Property>
				<Property>
                                        <Name>fpga_reprogram_size</Name>
                                        <Value>${fpga_reprogram_size}</Value>
                                </Property>
                                <Property>
                                        <Name>fpga_reprogram_str</Name>
                                        <Value>${fpga_reprogram_str}</Value>
                                </Property>
			 	<Property>
                                        <Name>network</Name>
                                        <Value>${network}</Value>
                                </Property>
                		<Property>
                    			<Name>estimated-creation-time</Name>
                    			<Value>10</Value>
                		</Property>
            		</Properties>
		<Images>
EOT
  if [ -z "${container_image}" ]; then
        cat >> "${PROJECT_FILE}" << EOT
            		<Image Name="None">
EOT
  else
	cat >> "${PROJECT_FILE}" << EOT
                        <Image Name="${container_image}">
EOT
  fi
  cat >> "${PROJECT_FILE}" <<EOT
				<InstallDir>${worker_install_dir}</InstallDir>
      				<WorkingDir>${worker_working_dir}</WorkingDir>
      				<Application>
                                        <AppDir>${appdir}</AppDir>
          				<LibraryPath>${library_path}</LibraryPath>
					<Classpath>${cp}</Classpath>
					<Pythonpath>${pythonpath}</Pythonpath>
      				</Application>
EOT
  if [ "$lot" != "" ] && [ "$lot" -ge 0 ]; then
    cat >> "${PROJECT_FILE}" <<EOT
      				<LimitOfTasks>${lot}</LimitOfTasks>

EOT
  fi

  cat >> "${PROJECT_FILE}" << EOT
  			</Image>
            </Images>
            <InstanceTypes>
                <InstanceType Name="default"/>
            </InstanceTypes>
        </CloudProvider>
    </Cloud>
EOT

}


###############################################
# Adds a compute node to Resources/Project
###############################################
add_compute_node() {
  local nodeName=$1
  local cus=$2
  local gpus=$3
  local fpgas=$4
  local lot=$5
  local memory=$6
  node=${nodeName}
  if [ ! -z "${NODE_NAME_QUEUE}" ]; then
        node=$(${NODE_NAME_QUEUE} "${node}")
  fi

  if [ "$CREATE_WORKING_DIRS" = true ]; then
      echo "Worker WD mkdir: ${LAUNCH_CMD} ${LAUNCH_PARAMS}${LAUNCH_SEPARATOR}${node} ${CMD_SEPARATOR}bash -c \"mkdir -p ${worker_working_dir} && chmod +rx ${worker_working_dir}\"${CMD_SEPARATOR}"
      #shellcheck disable=SC2086
      ${LAUNCH_CMD} ${LAUNCH_PARAMS}${LAUNCH_SEPARATOR}${node} ${CMD_SEPARATOR}bash -c "mkdir -p ${worker_working_dir} && chmod +rx ${worker_working_dir}"${CMD_SEPARATOR}
  fi

  nodeName=${nodeName}${network}

  cat >> "${RESOURCES_FILE}" << EOT
  <ComputeNode Name="${nodeName}">
      <Processor Name="MainProcessor">
          <ComputingUnits>${cus}</ComputingUnits>
          <Architecture>Intel</Architecture>
          <Speed>2.6</Speed>
      </Processor>
EOT

  if [ "${gpus}" != "$DEFAULT_GPUS_PER_NODE" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
        <Processor Name="GPU">
            <Type>GPU</Type>
            <ComputingUnits>${gpus}</ComputingUnits>
            <Architecture>k80</Architecture>
            <Speed>2.6</Speed>
        </Processor>
EOT
  fi

  if [ "${fpgas}" != "$DEFAULT_FPGAS_PER_NODE" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
        <Processor Name="FPGA">
            <Type>FPGA</Type>
            <ComputingUnits>${fpgas}</ComputingUnits>
            <Architecture>altera</Architecture>
            <Speed>1.0</Speed>
        </Processor>
EOT
  fi

  cat >> "${RESOURCES_FILE}" << EOT
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
          <Adaptor Name="es.bsc.compss.nio.master.NIOAdaptor">
              <SubmissionSystem>
                  <Interactive/>
              </SubmissionSystem>
              <Ports>
                  <MinPort>43001</MinPort>
                  <MaxPort>43002</MaxPort>
                  <RemoteExecutionCommand>${REMOTE_EXECUTOR}</RemoteExecutionCommand>
              </Ports>
          </Adaptor>
          <Adaptor Name="es.bsc.compss.gat.master.GATAdaptor">
              <SubmissionSystem>
                  <Interactive/>
              </SubmissionSystem>
              <BrokerAdaptor>sshtrilead</BrokerAdaptor>
          </Adaptor>
      </Adaptors>
EOT

  if [ "${GPFS2_PREFIX}" != "" ]; then
    cat >> "${RESOURCES_FILE}" << EOT
      <SharedDisks>
          <AttachedDisk Name="gpfs">
              <MountPoint>${GPFS_PREFIX}</MountPoint>
          </AttachedDisk>
EOT
    if [ "${GPFS2_PREFIX}" != "" ] && [ "${GPFS2_PREFIX}" != "${GPFS_PREFIX}" ]; then
      cat >> "${RESOURCES_FILE}" << EOT
          <AttachedDisk Name="gpfs2">
              <MountPoint>${GPFS2_PREFIX}</MountPoint>
          </AttachedDisk>
EOT
    fi

    cat >> "${RESOURCES_FILE}" << EOT
      </SharedDisks>
EOT
  fi
  cat >> "${RESOURCES_FILE}" << EOT
    </ComputeNode>
EOT

  cat >> "${PROJECT_FILE}" << EOT
  <ComputeNode Name="${nodeName}">
      <InstallDir>${worker_install_dir}</InstallDir>
      <WorkingDir>${worker_working_dir}</WorkingDir>
      <Application>
          <LibraryPath>${library_path}</LibraryPath>
      </Application>
EOT
  if [ "$lot" != "" ] && [ "$lot" -ge 0 ]; then
    cat >> "${PROJECT_FILE}" <<EOT
      <LimitOfTasks>${lot}</LimitOfTasks>
EOT
  fi

  cat >> "${PROJECT_FILE}" << EOT
  </ComputeNode>

EOT
}
