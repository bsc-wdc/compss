#!/bin/bash

if [ -n "${LOADED_SYSTEM_TRACE_GENPRV}" ]; then
  return 0
fi


# Checking COMPSs_HOME definition
if [ -z "${COMPSS_HOME}" ]; then
  echo "COMPSS_HOME not defined"
  exit 1
fi

# Load auxiliar scripts
# shellcheck source=../commons/java.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/commons/java.sh"



#-------------------------------------
# Define script variables and exports
#-------------------------------------
if [ -z "$EXTRAE_HOME" ]; then
  extraeDir="${COMPSS_HOME}/Dependencies/extrae/"
else
  extraeDir=$EXTRAE_HOME
fi

MIN_MPITS_PARALLEL_MERGE=1000
export LD_LIBRARY_PATH=$extraeDir/lib:$LD_LIBRARY_PATH

#-------------------------------------
# Constructs a PRV trace out of an mpits file.
# Parameters: 
# 1: mpits file to convert to PRV
# 2: path and name of the output prv file 
# 3: number of parallel processes to use to create the PRV trace.
#-------------------------------------
mpi2prv() {
  local mpits="${1}"
  local prv="${2}"
  local num_merge_procs="${3}"

  # Check machine max open files
  local openFilesLimit
  openFilesLimit=$(ulimit -Sn)
  local maxMpitNumber=0
  if [ "$openFilesLimit" -eq "$openFilesLimit" ] 2>/dev/null; then
    # ulimit reported a valid number of open files
    maxMpitNumber=$((openFilesLimit - 20))
  else
    maxMpitNumber=$MIN_MPITS_PARALLEL_MERGE
  fi
  
  # Check if parallel merge is available / should be used
  configuration=$("${extraeDir}"/etc/configured.sh | grep "enable-parallel-merge")
  if [ -z "${configuration}" ] || [ "${num_merge_procs}" -eq 1 ] || [ "$(wc -l < "${mpits}")" -lt ${maxMpitNumber} ] ; then
    "${extraeDir}/bin/mpi2prv" -f "${mpits}" -no-syn -o "${prv}"
  else
    mpirun -np "${num_merge_procs}" "${extraeDir}/bin/mpimpi2prv" -f "${mpits}" -no-syn -o "${prv}"
  fi
}

#-------------------------------------
# Constructs all the PRV files from the tace packages out of a COMPSs execution
# Parameters: 
# 1: path where to store the output prv file 
# 2: name of the output prv file 
# 3: number of parallel processes to use to create the PRV trace.
# >3: list of packages to join
#-------------------------------------
gen_traces() {
  local output_dir="${1}/"
  local trace_name="${2}"
  local num_merge_procs="${3}"
  shift 3
  local packages=${*}
  
  if [ ! -d "${output_dir}" ]; then
    mkdir -p "${output_dir}"
  fi
  python_output_dir="${output_dir}python/"
  if [ ! -d "${python_output_dir}" ]; then
    mkdir -p "${python_output_dir}"
  fi
  
  mpits="${output_dir}TRACE.mpits"
  prv="${output_dir}/${trace_name}.prv"
  
  set_folders=""
  for package in ${packages[*]}; do
    tmp_dir=$(mktemp -d)
    tar -C "${tmp_dir}" -xzf "${package}"
    
    #echo "trace:: $tmpDir -xvzf $file"
    if [ -f "${tmp_dir}/TRACE.mpits" ]; then
      hostId=$(cat "${tmp_dir}/hostID")
    
      cat "${tmp_dir}/TRACE.mpits" >> "${mpits}"
    
      set_folder=$(ls "${tmp_dir}" | grep "set" )
    
      cp -r "${tmp_dir}/${set_folder}" "${output_dir}"
      set_folders+=" ${output_dir}/${set_folder}"
    
      if [ -d "${tmp_dir}/python" ]; then
        hostId=$(cat "${tmp_dir}/hostID")
        python_mpits="${tmp_dir}/python/TRACE.mpits"
        python_prv="${python_output_dir}/${hostId}_python_trace.prv"
        mpi2prv "${python_mpits}" "${python_prv}" "${num_merge_procs}"
      fi
    
      if [ -f "${tmp_dir}/TRACE.sym" ]; then
      cp "${tmp_dir}/TRACE.sym" "${output_dir}"
      fi
    fi
    rm -rf "${tmp_dir}"
  done
  
  mpi2prv "${mpits}" "${prv}" "${num_merge_procs}"
  endCode=$?
  # cleaning 
  rm -rf "${mpits}" "${output_dir}/TRACE.sym"
  cd "${output_dir}"
  rm -rf set-*
}

#-------------------------------------
# Merges the events within python traces into the main one
# Parameters: 
# 1: directory where to find the main trace
# 2: name of the main prv file 
# >2: list of python traces to join into the main
#-------------------------------------
merge_python_traces() {
  local out_dir=${1}
  local trace_name=${2}
  shift 2
  local python_traces=${*}
  if [ -n "${python_traces}" ]; then
    ${JAVA} \
      -cp "${COMPSS_HOME}/Tools/tracing/compss-tracing.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
      "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/TraceMerging-log4j.${log_level}" \
      "-Dcompss.appLogDir=${specific_log_dir}" \
      es.bsc.compss.tracing.PythonTraceMerger \
      "${out_dir}" "${trace_name}" ${python_traces}
    endCode=$?
  else
    endCode=0
  fi
}

#-------------------------------------
# Reorganizes the threads of a trace
# Parameters: 
# 1: directory where to find the  trace
# 2: name of the trace
#-------------------------------------
rearrange_trace_threads() {
  local out_dir=${1}
  local trace_name=${2}
  
  ${JAVA} \
    -cp "${COMPSS_HOME}/Tools/tracing/compss-tracing.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
    "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/TraceMerging-log4j.${log_level}" \
    "-Dcompss.appLogDir=${specific_log_dir}" \
    es.bsc.compss.tracing.PrvSorter \
    "${out_dir}" "${trace_name}"
  endCode=$?
}


#-------------------------------------
# Joins several traces as a single one
# Parameters: 
# 1: path where to store the output prv file 
# 2: name of the output prv file 
# >2: directories of the agents 
#-------------------------------------
join_traces() {
  local out_dir=${1}
  local trace_name=${2}
  shift 2
  local agent_traces=${*}
  
  ${JAVA} \
    -cp "${COMPSS_HOME}/Tools/tracing/compss-tracing.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
    "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/TraceMerging-log4j.${log_level}" \
    "-Dcompss.appLogDir=${specific_log_dir}" \
    es.bsc.compss.tracing.AgentTraceMerger \
    "${out_dir}" "${trace_name}" ${agent_traces}
  endCode=$?
}

LOADED_SYSTEM_TRACE_GENPRV=1