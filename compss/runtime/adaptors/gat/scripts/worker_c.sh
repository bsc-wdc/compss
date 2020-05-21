#!/bin/bash

  # shellcheck disable=SC2154
  # Because many variables are sourced from the worker_commons.sh script

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
  echo "[WORKER_C.SH]       - method name                        = $methodName"

  arguments=(${invocation[@]:4})
  # shellcheck disable=SC2068
  get_invocation_params ${arguments[@]}

  # Pre-execution
  set_env

  compute_generic_sandbox
  echo "[WORKER_C.SH]       - sandbox                        = ${sandbox}"
  if [ ! -d "${sandbox}" ]; then
    mkdir -p "${sandbox}"
  fi

  workerConfDescription=( "${tracing}" "${taskId}" "${debug}" "${storageConf}" )
  implDescription=( "${implType}" "NULL" "${methodName}" "${timeout}" "$numSlaves" ${slaves[@]} "${cus}" "${hasTarget}" "${numResults}" "null" "${numParams}")
  
  invocationParams=( )

  totalParams=${#params[@]}
  index=0
  while [ "${index}" -lt "${totalParams}" ]; do
    type=${params[${index}]}
    stream=${params[$((index + 1))]}
    prefix=${params[$((index + 2))]}
    name=${params[$((index + 3))]}
    conType=${params[$((index + 4))]}
    weight=${params[$((index + 5))]}
    kr=${params[$((index + 6))]}
    case ${type} in
      [0-7]) #BASIC TYPE PARAM
        value=${params[$((index + 7))]}
        param=( "${type}" "${stream}" "${prefix}" "${name}" "${conType}" "${value}" )
        index=$((index + 8))
        ;;
      8)  # STRING PARAM
        lengthPos=$((index + 7))
        length=${params[${lengthPos}]}
        stringValue=${params[@]:$((index + 8)):${length}}
        param=( "${type}" "${stream}" "${prefix}" "${name}" "${conType}" "${length}" "${stringValue[@]}" )
        index=$((index + length + 8))
        ;;
      9) # FILE PARAM
        originalNameIdx=$((index + 7))
        dataLocationIdx=$((index + 8))
        originalName=${params[$originalNameIdx]}
        dataLocation=${params[${dataLocationIdx}]}
        if [ "$kr" = "false" ]; then
        	moveFileToSandbox "${dataLocation}" "${originalName}"
        	param=( "${type}" "${stream}" "${prefix}" "${name}" "${conType}" "${sandbox}/${originalName}" )
        else 
            param=( "${type}" "${stream}" "${prefix}" "${name}" "${conType}" "${dataLocation}" )
        fi
        index=$((index + 9))
        ;;
      13) #BINDING OBJECT
        bo_id=${params[$((index + 7))]}
        bo_type=${params[$((index + 8))]}
        bo_elements=${params[$((index + 9))]}
        param=( "${type}" "${stream}" "${prefix}" "${name}" "${conType}" "${bo_id}" "${bo_type}" "${bo_elements}" )
        index=$((index + 10))
      ;;
      *)
        value=${params[$((index + 7))]}
        write=${params[$((index + 8))]}
        param=( "${type}" "${stream}" "${prefix}" "${name}" "${conType}" "${value}" "${write}")
        index=$((index + 9))
        ;;
    esac
    invocationParams=( ${invocationParams[@]} ${param[@]} )
  done

  echo "[WORKER_C.SH] EXEC CMD: ${appDir}/worker/worker_c ${workerConfDescription[*]} ${implDescription[*]} ${invocationParams[*]}"
  # shellcheck disable=SC2068
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
