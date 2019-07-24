#!/bin/bash

  # shellcheck disable=SC2154
  # Because many variables are initialized by the worker_commons.sh script

  ####################################
  # WORKER SPECIFIC HELPER FUNCTIONS #
  ####################################

  activate_virtual_environment () {
    # shellcheck source=./bin/activate
    # shellcheck disable=SC1091
    source "${pythonVirtualEnvironment}"/bin/activate
  }

  deactivate_virtual_environment () {
    # 'deactivate' function included in virtual environment activate script
    deactivate
  }

  ####################################
  #               MAIN               #
  ####################################

  # Get worker common functions
  SCRIPT_DIR=$(dirname "$0")

  # shellcheck source=./worker_commons.sh
  # shellcheck disable=SC1091
  source "${SCRIPT_DIR}"/worker_commons.sh

  #-------------------------------------
  # Retrieve host configuration
  #-------------------------------------
  get_host_parameters "$@"

  implType="${invocation[0]}"
  lang="${invocation[1]}"
  cp="${invocation[2]}"
  methodName="${invocation[3]}"
  echo "[WORKER_PYTHON.SH]    - method name                        = $methodName"

  arguments=(${invocation[@]:4})
  get_invocation_params ${arguments[@]}

  # Pre-execution
  set_env

  compute_generic_sandbox
  echo "[WORKER_PYTHON.SH]    - sandbox                        = ${sandbox}"
  if [ ! -d "${sandbox}" ]; then
    mkdir -p "${sandbox}"
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
 
  # shellcheck disable=SC2206 
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
  # shellcheck disable=SC2206
  implDescription=( "${implType}" "${moduleName}" "${methodName}" "$numSlaves" ${slaves[@]} "${cus}" "${hasTarget}" "null" "${numResults}" "${paramCount}")


  invocationParams=( )
  totalParams=${#params[@]}
  index=0
  while [ "${index}" -lt "${totalParams}" ]; do
    ptype=${params[${index}]}
    stream=${params[$((index + 1))]}
    prefix=${params[$((index + 2))]}
    name=${params[$((index + 3))]}
    case ${ptype} in
      [0-7]) 
        value=${params[$((index + 4))]}
        param=( "${ptype}" "${stream}" "${prefix}" "${name}" "${value}" )
        index=$((index + 5))
        ;;
      8) 
        lengthPos=$((index + 4))
        length=${params[${lengthPos}]}
        stringValue=${params[@]:$((index + 5)):${length}}
        param=( "${ptype}" "${stream}" "${prefix}" "${name}" "${length}" "${stringValue[@]}" )
        index=$((index + length + 5))
        ;;
      9)
        originalNameIdx=$((index + 4))
        dataLocationIdx=$((index + 5))
        originalName=${params[$originalNameIdx]}
        dataLocation=${params[${dataLocationIdx}]}
        moveFileToSandbox "${dataLocation}" "${originalName}"
        param=( "${ptype}" "${stream}" "${prefix}" "${name}" "${sandbox}/${originalName}" )
        index=$((index + 6))

        ;;
      *)
        value=${params[$((index + 4))]}
        write=${params[$((index + 5))]}
        param=( "${ptype}" "${stream}" "${prefix}" "${name}" "${value}" "${write}")
        index=$((index + 6))
        ;;
    esac
    invocationParams=( ${invocationParams[@]} ${param[@]} )
  done

  # Include version subfolder in pycompss home and set pythonpath related env
  export PYCOMPSS_HOME=${PYCOMPSS_HOME}/${pythonVersion}
  export PYTHONPATH=${PYCOMPSS_HOME}:${pythonpath}:${app_dir}:${PYTHONPATH}

  echo "[WORKER_PYTHON.SH] PYTHONPATH: ${PYTHONPATH}"
  echo "[WORKER_PYTHON.SH] EXEC CMD: $pythonInterpreter ${PYCOMPSS_HOME}/pycompss/worker/gat/worker.py ${workerConfDescription[*]} ${implDescription[*]} ${invocationParams[*]}"

  # shellcheck disable=SC2068
  $pythonInterpreter "${PYCOMPSS_HOME}"/pycompss/worker/gat/worker.py ${workerConfDescription[@]} ${implDescription[@]} ${invocationParams[@]}
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
