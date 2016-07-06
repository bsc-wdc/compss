#!/bin/bash

  ############################################################
  # SCRIPT FOR SUBMISSION OF APPLICATIONS TO LSF WITH COMPSs #
  ############################################################

  ###############################
  # CONSTANTS
  ###############################
  ERROR_NUM_NODES="Incorrect number of nodes requested. MININUM 2."
  ERROR_TMP_FILE="Cannnot create tmp file"
  ERROR_SUBMIT_SCRIPT="Cannot create the submit script"
  ERROR_SUBMIT="Cannot submit the job"
  ERROR_TASKS_PER_NODE="Incorrect number of tasks per node. MINIMUM 1."

  ###############################
  # FUNCTIONS
  ###############################
  # Function that converts a cost in minutes to an expression of wall clock limit for slurm
  convert_to_wc() {
        local cost=$1
        wc_limit=""

        local min=`expr $cost % 60`
        if [ $min -lt 10 ]; then
                wc_limit=":0${min}${wc_limit}"
        else
                wc_limit=":${min}${wc_limit}"
        fi

        local hrs=`expr $cost / 60`
        if [ $hrs -gt 0 ]; then
                if [ $hrs -lt 10 ]; then
                        wc_limit="0${hrs}${wc_limit}"
                else
                        wc_limit="${hrs}${wc_limit}"
                fi
        else
                wc_limit="00${wc_limit}"
        fi
  }

  display_error() {
	local errorMsg=$1
	local exitValue=$2

	echo " "
	echo "ERROR: $errorMsg"
	echo " "

        echo "Exiting..."
	exit $exitValue
  }


  ###############################
  # MAIN PROGRAM
  ###############################
  #Get script parameters
  queue=$1
  reservation=$2
  convert_to_wc $3
  dependencyJob=$4
  num_nodes=$5
  num_switches=$6
  tasks_per_node=$7
  node_memory=$8
  network=$9
  master_port=${10}
  master_working_dir=${11}
  jvm_master_opts=${12}
  worker_working_dir=${13}
  jvm_workers_opts=${14}
  worker_in_master_tasks=${15}
  worker_in_master_memory=${16}
  jvm_worker_in_master_opts=${17}
  library_path=${18}
  cp=${19}
  pythonpath=${20}
  lang=${21}
  log_level=${22}
  tracing=${23}
  comm=${24}
  storageName=${25}
  storageConf=${26}
  taskExecution=${27}
  shift 27

  #Display arguments
  echo "Queue:                     ${queue}"
  echo "Reservation:	           ${reservation}"
  echo "Num Nodes:                 ${num_nodes}"
  echo "Num Switches:              ${num_switches}"
  echo "Job dependency:            ${dependencyJob}"
  echo "Exec-Time:                 ${wc_limit}"
  echo "Network:                   ${network}"
  echo "Node memory:	           ${node_memory}"
  echo "Tasks per Node:            ${tasks_per_node}"
  echo "Worker in Master Tasks:    ${worker_in_master_tasks}"
  echo "Worker in Master Memory:   ${worker_in_master_memory}"
  echo "Master Port:               ${master_port}"
  echo "Master WD:                 ${master_working_dir}"
  echo "Worker WD:                 ${worker_working_dir}"
  echo "Master JVM Opts:           ${jvm_master_opts}"
  echo "Workers JVM Opts:          ${jvm_workers_opts}"
  echo "Worker in Master JVM Opts: ${jvm_worker_in_master_opts}"
  echo "Library Path:              ${library_path}"
  echo "Classpath:                 ${cp}"  
  echo "Pythonpath:                ${pythonpath}"
  echo "Lang:                      ${lang}"
  echo "COMM:                      ${comm}"
  echo "Storage name:	           ${storageName}"
  echo "Storage conf:	           ${storageConf}"
  echo "Task execution:	           ${taskExecution}"
  echo "To COMPSs:                 $*"
  echo " "
  
  #Check arguments
  if [ ${num_nodes} -lt 2 ]; then
     display_error "${ERROR_NUM_NODES}" 1
  fi
  if [ ${tasks_per_node} -lt 1 ]; then
     display_error "${ERROR_TASKS_PER_NODE}" 1
  fi

  #Create TMP DIR for submit script
  TMP_SUBMIT_SCRIPT=$(mktemp)
  echo "Temp submit script is: $TMP_SUBMIT_SCRIPT"
  if [ $? -ne 0 ]; then
	display_error "${ERROR_TMP_FILE}" 1
  fi

  #Create submit script
  script_dir=$(dirname $0)
  IT_HOME=${script_dir}/../../../..

  if [ "${queue}" != "default" ]; then
    /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#!/bin/bash
#
#BSUB -q ${queue}
EOT
  else 
    /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#!/bin/bash
#
EOT
  fi

  if [ "${num_switches}" != "0" ]; then
    /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#BSUB -R "cu[maxcus=${num_switches}]"
EOT
  fi

  if [ "${dependencyJob}" != "None" ]; then
    /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#BSUB -J COMPSs -w 'ended(${dependencyJob})'
EOT
  else 
    /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#BSUB -J COMPSs
EOT
  fi

  if [ "${reservation}" != "disabled" ]; then
    /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#BSUB -U ${reservation}
EOT
  fi

  if [ "${node_memory}" != "disabled" ]; then
    /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#BSUB -M ${node_memory}
EOT
  fi

  /bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#BSUB -cwd ${master_working_dir} 
#BSUB -oo compss-%J.out
#BSUB -eo compss-%J.err
#BSUB -n ${num_nodes}
#BSUB -R "span[ptile=1]" 
#BSUB -W $wc_limit 

specific_log_dir=$HOME/.COMPSs/\${LSB_JOBID}/
mkdir -p \${specific_log_dir}

${script_dir}/launch.sh $IT_HOME \$LSB_DJOB_HOSTFILE ${tasks_per_node} ${worker_in_master_tasks} ${worker_in_master_memory} ${worker_working_dir} "\${specific_log_dir}" "${jvm_master_opts}" "${jvm_workers_opts}" "${jvm_worker_in_master_opts}" ${network} ${node_memory} ${master_port} ${library_path} ${cp} ${pythonpath} ${lang} ${log_level} ${tracing} ${comm} ${storageName} ${storageConf} ${taskExecution} $@
EOT

  # Check if the creation of the script failed
  result=$?
  if [ $result -ne 0 ]; then
	display_error "${ERROR_SUBMIT_SCRIPT}" 1
  fi

  if [ "${taskExecution}" != "compss" ]; then
      echo "Running in COMPSs and Storage mode."
	  # Run directly the script
	  /bin/chmod +x ${TMP_SUBMIT_SCRIPT}
	  ${TMP_SUBMIT_SCRIPT}
  else
      echo "Running in COMPSs mode."
	  # Submit the job to the queue
	  bsub < ${TMP_SUBMIT_SCRIPT} 1>${TMP_SUBMIT_SCRIPT}.out 2>${TMP_SUBMIT_SCRIPT}.err
	  result=$?
  fi

  # Cleanup
  submit_err=$(cat ${TMP_SUBMIT_SCRIPT}.err)
  rm -rf ${TMP_SUBMIT_SCRIPT}.*

  # Check if submission failed
  if [ $result -ne 0 ]; then
	display_error "${ERROR_SUBMIT}${submit_err}" 1
  fi

