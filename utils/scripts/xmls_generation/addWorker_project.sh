#!/bin/bash

  usage() {
    echo "Usage: "
    echo "    $0 <workerUser> <workerIP> <projectFile>"
    echo " "
  }

  write_to_file() {
    local user=$1
    local workerName=$2
    local file=$3

    cat >> ${file} << EOT

    <ComputeNode Name="${workerName}">
        <InstallDir>/opt/COMPSs/</InstallDir>
        <WorkingDir>/tmp/${workerName}/</WorkingDir>
        <!-- <Application> -->
        <!--     <AppDir></AppDir> -->
        <!--     <LibraryPath></LibraryPath> -->
        <!--     <Classpath></Classpath> -->
        <!--     <Pythonpath></Pythonpath> -->
        <!-- </Application> -->
        <!-- <LimitOfTasks></LimitOfTasks> -->
        <Adaptors>
            <Adaptor Name="integratedtoolkit.nio.master.NIOAdaptor">
                <SubmissionSystem>
                    <Interactive/>
                </SubmissionSystem>
                <Ports>
                    <MinPort>43001</MinPort>
                    <MaxPort>43002</MaxPort>
                </Ports>
EOT

    if [ "${user}" != "" ]; then
    cat >> ${file} << EOT
                <User>${user}</User>
EOT
    fi

    cat >> ${file} << EOT
            </Adaptor>
            <Adaptor Name="integratedtoolkit.gat.master.GATAdaptor">
                <SubmissionSystem>
                    <Batch>
                        <Queue>sequential</Queue>
                        <Queue>training</Queue>
                    </Batch>
                    <Interactive/>
                </SubmissionSystem>
EOT

    if [ "${user}" != "" ]; then
    cat >> ${file} << EOT
                <User>${user}</User>
EOT
    fi

    cat >> ${file} << EOT
                <BrokerAdaptor>sshtrilead</BrokerAdaptor>
            </Adaptor>
        </Adaptors>
    </ComputeNode>
EOT

  }

  ###########################################################
  # MAIN
  ###########################################################

  # Arguments
  # echo "[ADD_WORKER] [DEBUG] Parsing arguments"
  if [ $# -ne 3 ]; then
    echo "ERROR: Invalid number of arguments"
    usage
    exit 1
  fi
  workerUser=$1
  workerIP=$2
  projectFile=$3

  # Appending to file
  echo "[ADD_WORKER] [DEBUG] Adding ${workerIP} to ${projectFile} with: "
  echo "[ADD_WORKER] [DEBUG]    - User:		$workerUser"

  write_to_file "${workerUser}" "${workerIP}" "${projectFile}"

  # DONE
  # echo "[ADD_WORKER] [INFO] DONE"
  exit 0
 
