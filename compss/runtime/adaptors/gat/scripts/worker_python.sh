#!/bin/bash

  ####################################
  # WORKER SPECIFIC HELPER FUNCTIONS #
  ####################################

  activate_virtual_environment () {
    source "${pythonVirtualEnvironment}"/bin/activate
  }

  deactivate_virtual_environment () {
    # 'deactivate' function included in virtual environment activate script
    deactivate
  }

  ####################################
  #               MAIN               #
  ####################################

  #!/bin/bash

  # Get worker common functions
  SCRIPT_DIR=$(dirname "$0")

  # shellcheck source=./worker_commons.sh
  source "${SCRIPT_DIR}"/worker_commons.sh

  #-------------------------------------
  # Retrieve host configuration
  #-------------------------------------
  get_host_parameters "$@"

  implType=${invocation[0]}
  lang=${invocation[1]}
  cp=${invocation[2]}
  methodName=${invocation[3]}
  echo "[WORKER_PYTHON.SH]    - method name                        = $methodName"


  arguments=(${invocation[@]:4})
  get_invocation_params ${arguments[@]}

  # Pre-execution
  set_env


  compute_generic_sandbox
  echo "[WORKER_PYTHON.SH]    - sandbox                        = ${sandbox}"
  if [ ! -d ${sandbox} ]; then
    mkdir -p ${sandbox}
  fi


  implType=${invocation[0]}
  lang=${invocation[1]}
  
  # Pre-execution
  pythonpath=${invocation[2]}
  pythonInterpreter=${invocation[3]}
  pythonVersion=${invocation[4]}
  pythonVirtualEnvironment=${invocation[5]}
  pythonPropagateVirtualEnvironment=${invocation[6]}
  if [ "${debug}" == "true" ]; then
    echo "[WORKER_PYTHON.SH] - pythonpath                         = $pythonpath"
    echo "[WORKER_PYTHON.SH] - pythonInterpreter                  = $pythonInterpreter"
    echo "[WORKER_PYTHON.SH] - pythonVersion                      = $pythonVersion"
    echo "[WORKER_PYTHON.SH] - pythonVirtualEnvironment           = $pythonVirtualEnvironment"
    echo "[WORKER_PYTHON.SH] - pythonPropagateVirtualEnvironment  = $pythonPropagateVirtualEnvironment"
  fi
  
  


  arguments=(${invocation[@]:9})
  moduleName=${invocation[7]}
  methodName=${invocation[8]}
  get_invocation_params ${arguments[@]}

  # Pre-execution
  set_env


  workerConfDescription=( "${tracing}" "${taskId}" "${debug}" "${storageConf}" )
  if [ "${hasTarget}" == "true" ]; then
    paramCount=$((numParams + 1))
  else
    paramCount=$((numParams))
  fi
  paramCount=$((paramCount + numResults))
  implDescription=( "${implType}" "${moduleName}" "${methodName}" "$numSlaves" ${slaves[@]} "${cus}" "${hasTarget}" "null" "${numResults}" "${paramCount}")
  compute_generic_sandbox
  echo "[WORKER_PYTHON.SH]    - sandbox                        = ${sandbox}"
  if [ ! -d ${sandbox} ]; then
    mkdir -p ${sandbox}
  fi



  invocationParams=( )

  totalParams=${#params[@]}
  index=0
  while [ ${index} -lt ${totalParams} ]; do
    type=${params[${index}]}
    stream=${params[$((index + 1))]}
    prefix=${params[$((index + 2))]}
    case ${type} in
      [0-7]) 
        value=${params[$((index + 3))]}
        param=( "${type}" "${stream}" "${prefix}" "${value}" )
        index=$((index + 4))
        ;;
      8) 
        lengthPos=$((index + 3))
        length=${params[${lengthPos}]}
        stringValue=${params[@]:$((index + 4)):${length}}
        param=( "${type}" "${stream}" "${prefix}" "${length}" "${stringValue[@]}" )
        index=$((index + length + 4))
        ;;
      9)
        originalNameIdx=$((index + 3))
        dataLocationIdx=$((index + 4))
        originalName=${params[$originalNameIdx]}
        dataLocation=${params[${dataLocationIdx}]}
        moveFileToSandbox ${dataLocation} ${originalName}
        param=( "${type}" "${stream}" "${prefix}" "${sandbox}/${originalName}" )
        index=$((index + 5))

        ;;
      *)
        value=${params[$((index + 3))]}
        write=${params[$((index + 4))]}
        param=( "${type}" "${stream}" "${prefix}" "${value}" "${write}")
        index=$((index + 5))
        ;;
    esac
    invocationParams=( ${invocationParams[@]} ${param[@]} )
  done




  # Include version subfolder in pycompss home and set pythonpath related env
  export PYCOMPSS_HOME=${PYCOMPSS_HOME}/${pythonVersion}
  export PYTHONPATH=${PYCOMPSS_HOME}:${pythonpath}:${app_dir}:${PYTHONPATH}

  echo "[WORKER_PYTHON.SH] PYTHONPATH: ${PYTHONPATH}"
  echo "[WORKER_PYTHON.SH] EXEC CMD: $pythonInterpreter ${PYCOMPSS_HOME}/pycompss/worker/worker.py ${workerConfDescription[@]} ${implDescription[@]} ${invocationParams[@]}"
  $pythonInterpreter "${PYCOMPSS_HOME}"/pycompss/worker/worker.py ${workerConfDescription[@]} ${implDescription[@]} ${invocationParams[@]}
  ev=$?

  #-------------------------------------
  # Clean sandbox
  #-------------------------------------

  moveFilesOutFromSandbox

  if [ "${isSpecific}" != "true" ]; then
    rm -rf "${sandbox}"
  fi


  # Exit
  if [ $ev -eq 0 ]; then
    exitCode=0
  else
    echo 1>&2 "Task execution failed"
    exitCode=7
  fi

  if [ "$pythonVirtualEnvironment" != "null" ] && [ "$pythonPropagateVirtualEnvironment" == "true" ]; then
    deactivate_virtual_environment
  fi

  exit $exitCode
