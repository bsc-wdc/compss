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
  echo "[WORKER_C.SH]    - method name                        = $methodName"


  arguments=(${invocation[@]:4})
  get_invocation_params ${arguments[@]}

  # Pre-execution
  set_env


  compute_generic_sandbox
  echo "[WORKER_C.SH]    - sandbox                        = ${sandbox}"
  if [ ! -d ${sandbox} ]; then
    mkdir -p ${sandbox}
  fi

  
  workerConfDescription=( "${tracing}" "${taskId}" "${debug}" "${storageConf}" )
  implDescription=( "${implType}" "NULL" "${methodName}" "$numSlaves" ${slaves[@]} "${cus}" "${hasTarget}" "${numResults}" "null" "${numParams}")
  
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

  echo "[WORKER_C.SH] EXEC CMD: ${appDir}/worker/worker_c ${workerConfDescription[@]} ${implDescription[@]} ${invocationParams[@]}"
  "${appDir}"/worker/worker_c ${workerConfDescription[@]} ${implDescription[@]} ${invocationParams[@]}
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
    exit 0
  else
    echo 1>&2 "Task execution failed"
    exit 7
  fi

