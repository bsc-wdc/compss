#!/bin/bash -e


  #
  # HELPER FUNCTIONS
  #

  #
  # MAIN
  #

  # Script global variables
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Script arguments
  job_log_file=${1:-experiments.log}
  job_results_file=${2:-./results/mn/results.summary}
  move_traces=${3:-true}

  # Initialize results log file
  echo -e "JOB_ID\tVERSION\t\tNSIZE\tTRACING\tNUM_WORKERS\tTOTAL_TIME\tINIT_TIME\tCOMP_TIME\tNUM_TASKS" > "${job_results_file}"

  first=0
  while read -r line; do
    # Skip header
    if [ "$first" -eq 0 ]; then
      first=1
      continue
    fi

    # Get job log file information
    job_id=$(echo "$line" | awk '{ print $1 }')
    version=$(echo "$line" | awk '{ print $2 }')
    nsize=$(echo "$line" | awk '{ print $3 }')
    tracing=$(echo "$line" | awk '{ print $4 }')

    # Get job output information
    job_output=${SCRIPT_DIR}/results/mn/${version}/compss-${job_id}.out
    num_workers=$(grep -c "Worker WD mkdir" "${job_output}")
    total_time=$(grep "TOTAL_TIME" "${job_output}" | awk '{ print $NF }' | cat)
    init_time=$(grep "INIT_TIME" "${job_output}" | awk '{ print $NF }' | cat)
    comp_time=$(grep "COMP_TIME" "${job_output}" | awk '{ print $NF }' | cat)
    num_tasks=$(grep "Total executed tasks:" "${job_output}" | awk '{ print $NF }' | cat)

    # Print results
    echo -e "${job_id}\t${version}\t${nsize}\t${tracing}\t${num_workers}\t\t${total_time}\t${init_time}\t${comp_time}\t${num_tasks}" >> "${job_results_file}"

    # Move traces to its location
    if [ "${move_traces}" == "true" ] && [ "$tracing" == "true" ]; then
      trace_path=${SCRIPT_DIR}/results/mn/${version}/.COMPSs/${job_id}/trace
      new_trace_path=${SCRIPT_DIR}/results/mn/${version}/trace-${job_id}
      new_trace_basename=fdtd2d-${version}-${job_id}-${nsize}
      mkdir -p "${new_trace_path}"
      if [[ $(find "${trace_path}" -name "*.prv") != "" ]]; then
        cp "${trace_path}"/*.prv "${new_trace_path}"/"${new_trace_basename}".prv
        cp "${trace_path}"/*.pcf "${new_trace_path}"/"${new_trace_basename}".pcf
        cp "${trace_path}"/*.row "${new_trace_path}"/"${new_trace_basename}".row
      fi
    fi
  done < "${job_log_file}"
