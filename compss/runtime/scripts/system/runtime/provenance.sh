run_provenance() {
  if [ -z "${ENQUEUE_COMPSS_ARGS}" ]; then
    echo "runcompss" $all_arguments > .compss_submission_command_line.txt
  else
    echo "enqueue_compss ${ENQUEUE_COMPSS_ARGS}" > .compss_submission_command_line.txt
  fi

  # Stop profiling process
  stop_profiling
  echo "PROVENANCE | PROFILING | Profiling stopped."

  mkdir -p "${specific_log_dir}/stats/"
  find "${specific_log_dir}/workers/" -type f -name "*.csv" -exec cp {} "${specific_log_dir}/stats/" \;
  find "${specific_log_dir}/workers/" -type f -name "*.log" -exec cp {} "${specific_log_dir}/stats/" \;
  for file in "${specific_log_dir}"stats/*.csv; do
    if [[ "$(basename "$file")" == *"${COMPSS_MASTER_NODE}"* ]]; then
      new_name="${file%.csv}-MASTER.csv"
      mv "$file" "$new_name"
      break
    fi
  done

  # Check if provenance_dest_path is unset or empty
  if [ -z "${provenance_dest_path}" ]; then
      # Create a timestamp in the format YYYYMMDD_HHMMSS
      timestamp=$(date +%Y%m%d_%H%M%S)
      # Set the variable with the desired naming scheme
      provenance_dest_path="COMPSs_RO-Crate_${timestamp}/"
  fi

  echo "PROVENANCE | STARTING WORKFLOW PROVENANCE SCRIPT"
  echo "PROVENANCE | Destination folder: ${provenance_dest_path}"
  echo "PROVENANCE | If needed, Provenance generation can be triggered by hand using the following commands:"
  echo -e "\t${COMPSS_HOME}/Runtime/scripts/utils/compss_gengraph svg ${specific_log_dir}/monitor/complete_graph.dot"
  echo -e "\texport PYTHONPATH=${COMPSS_HOME}/Runtime/scripts/system/:\$PYTHONPATH"
  echo -e "\tpython3 ${COMPSS_HOME}/Runtime/scripts/system/provenance/generate_COMPSs_RO-Crate.py ${provenance_yaml} ${specific_log_dir} ${provenance_dest_path} ${zip_provenance}"

  if [ ! -z "${BSC_MACHINE}" ]; then
    echo "PROVENANCE | TIP for BSC cluster users: before triggering generation by hand, run first: salloc --account=<your_group> --qos=gp_debug -p interactive"
  fi
  echo "PROVENANCE | Generating graph for Workflow Provenance"
  edges=`grep "\->" ${specific_log_dir}/monitor/complete_graph.dot | wc -l`
  echo "PROVENANCE | Number of edges in the graph: ${edges}"
  if [ ${edges} -lt 6500 ]; then
    start=`date +%s`
    "${COMPSS_HOME}/Runtime/scripts/utils/compss_gengraph" "svg" "${specific_log_dir}/monitor/complete_graph.dot"
    end=`date +%s`
    echo "PROVENANCE | Ended generating graph for Workflow Provenance. TIME: $(( end - start )) s"
  else
    echo "PROVENANCE | WARNING: the workflow has an extremely large number of edges. Aborting workflow diagram generation"
  fi

  echo "PROVENANCE | STARTING RO-CRATE GENERATION SCRIPT"
  export PYTHONPATH=${COMPSS_HOME}/Runtime/scripts/system/:${PYTHONPATH}
  if [ ! -z "${COMPSS_PROV_DEBUG}" ]; then
    python3 "${COMPSS_HOME}/Runtime/scripts/system/provenance/generate_COMPSs_RO-Crate.py" "${provenance_yaml}" "${specific_log_dir}" "${provenance_dest_path}" "${zip_provenance}"
  else
    python3 -O "${COMPSS_HOME}/Runtime/scripts/system/provenance/generate_COMPSs_RO-Crate.py" "${provenance_yaml}" "${specific_log_dir}" "${provenance_dest_path}" "${zip_provenance}"
  fi
  if [ $? == 0 ]; then # Check if generation has ended correctly, if not, do not cleanup
    rm -f .compss_submission_command_line.txt "${specific_log_dir}/monitor/complete_graph.svg"
  else
    echo "PROVENANCE | WORKFLOW PROVENANCE GENERATION HAS FAILED"
    echo "PROVENANCE | Temporary files have not been erased: .compss_submission_command_line.txt ${specific_log_dir}/monitor/complete_graph.svg"
  fi
  echo "PROVENANCE | ENDED WORKFLOW PROVENANCE SCRIPT"
}