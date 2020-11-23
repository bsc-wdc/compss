#!/bin/bash -e

  # Obtain parameters
  output_file=$1
  error_file=$2
  target_log_folder=$3

  # Log files
  runtime_log="${target_log_folder}/runtime.log"
  resources_log="${target_log_folder}/resources.log"

  #----------------------------------------------------------------------------------
  # Check output standard out status
  if [ -f "${output_file}" ]; then
     result_expected="------------------------------------------------------------"
     test_result=$(tail -1 "${output_file}")
     if [ "${result_expected}" != "${test_result}" ]; then
        echo "[OK] THE EXECUTION FAILED, AS IT SHOULD" | tee -a "${output_file}"
        exit 0
     fi
  else
     echo "[ERROR] Output file not found" | tee -a "${output_file}"
     exit 1
  fi
  echo "[ERROR] THE EXECUTION SHOULD HAVE FAILED" | tee -a "${output_file}"
  exit 1
