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

###############################################
###############################################
#            CONSTANTS DEFINITION
###############################################
###############################################
GEN_TRACING_LOG_LEVEL_OFF="off"
GEN_TRACING_LOG_LEVEL_DEBUG="debug"
GEN_TRACING_DEFAULT_LOG_LEVEL="${GEN_TRACING_LOG_LEVEL_OFF}"

GEN_TRACING_DEFAULT_LOG_DIR="."

#-------------------------------------
# Define script variables and exports
#-------------------------------------
if [ -z "$EXTRAE_HOME" ]; then
  extraeDir="${COMPSS_HOME}/Dependencies/extrae/"
else
  extraeDir=$EXTRAE_HOME
fi

command_exists () {
  type "$1" &> /dev/null ;
}

if command_exists "${extraeDir}/bin/mpi2prv" ; then
  mpi2prv_bin="${extraeDir}/bin/mpi2prv"
  mpimpi2prv_bin="${extraeDir}/bin/mpimpi2prv"
elif command_exists "${extraeDir}/bin/x86_64-linux-gnu-mpi2prv" ; then
  mpi2prv_bin="${extraeDir}/bin/x86_64-linux-gnu-mpi2prv"
  mpimpi2prv_bin="${extraeDir}/bin/x86_64-linux-gnu-mpimpi2prv"
else
  echo "ERROR: Could not find mpi2prv or x86_64-linux-gnu-mpi2prv binary."
  exit 1
fi

MIN_MPITS_PARALLEL_MERGE=1000
export LD_LIBRARY_PATH=$extraeDir/lib:$LD_LIBRARY_PATH

#-------------------------------------
# Assigns default values to undefined mandatory variables
#-------------------------------------
check_genPRV_env(){
  if [ -z "${gen_tracing_log_level}" ]; then
    gen_tracing_log_level=${GEN_TRACING_DEFAULT_LOG_LEVEL}
  fi

  if [ ! "${gen_tracing_log_level}" == "${GEN_TRACING_LOG_LEVEL_OFF}" ]; then
    gen_tracing_log_level="${GEN_TRACING_LOG_LEVEL_DEBUG}"
    if [ -z "${gen_tracing_log_dir}" ]; then
      gen_tracing_log_dir=${GEN_TRACING_DEFAULT_LOG_DIR}
    fi
  fi

  if [ ! "${gen_tracing_log_dir: -1}" == "/" ]; then
    gen_tracing_log_dir="${gen_tracing_log_dir}/"
  fi
}

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
    "${mpi2prv_bin}" -f "${mpits}" -no-syn -o "${prv}"
  else
    mpirun -np "${num_merge_procs}" "${mpimpi2prv_bin}" -f "${mpits}" -no-syn -o "${prv}"
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
  R_output_dir="${output_dir}R/"
  if [ ! -d "${R_output_dir}" ]; then
    mkdir -p "${R_output_dir}"
  fi

  mpits="${output_dir}TRACE.mpits"
  prv="${output_dir}${trace_name}.prv"

  set_folders=""
  for package in ${packages[*]}; do
    tmp_dir=$(mktemp -d)
    tar -C "${tmp_dir}" -xzf "${package}"

    hostId=$(cat "${tmp_dir}/hostID")

    # DEAL WITH JAVA TRACE
    if [ -f "${tmp_dir}/TRACE.mpits" ]; then
      sed -i "s|//|/|g" "${tmp_dir}/TRACE.mpits"
      local original_absolute_path=""
      for f in $(tar -tzf ${package} | grep .mpit | grep -v mpits); do
        f=$(echo $f | cut -c2-)
        grep=$(grep "${f}" "${tmp_dir}/TRACE.mpits" | awk '{print $1}')
        original_absolute_path=${grep//$f/}
        if [ -n "${original_absolute_path}" ]; then
          break
        fi
      done

      sed -i "s|${original_absolute_path}|${output_dir}|g" "${tmp_dir}/TRACE.mpits"
      cat "${tmp_dir}/TRACE.mpits" >> "${mpits}"

      set_folder=$(ls "${tmp_dir}" | grep "set" )

      cp -r "${tmp_dir}/${set_folder}" "${output_dir}"
      set_folders+=" ${output_dir}/${set_folder}"

      if [ -f "${tmp_dir}/TRACE.sym" ]; then
      cp "${tmp_dir}/TRACE.sym" "${output_dir}"
      fi
    else
      echo "Java trace information not found" 1>&2
    fi

    # DEAL WITH PYTHON TRACE
    python_dir="${tmp_dir}/python"

    missing_mpits=""
    if [ -d "${python_dir}" ]; then
      python_mpits="${python_dir}/TRACE.mpits"
      sed -i "s|//|/|g" "${python_mpits}"
      if [ -f "${python_mpits}" ]; then
        local original_absolute_path=""
        # Find missing mpits in TRACE.mpits
        for f in $(tar -tzf ${package} | grep python | grep .mpit | grep -v mpits); do
          f=$(echo $f | cut -c2-)
          grep=$(grep "${f}" "${python_mpits}" | awk '{print $1}')
          original_absolute_path=${grep//$f/}
          if [ -z "${original_absolute_path}" ]; then
            echo "mpit file ${f} not included in the original mpits file. Adding it..." 1>&2
            missing_mpits="${missing_mpits}--\n${tmp_dir}${f} named\n"
          fi
        done
        sed -i "s|${original_absolute_path}/python|${python_dir}|g" "${python_mpits}"
        echo -e "${missing_mpits}" >> "${python_mpits}"
        # Update .libseqtrace-subprocess.so path in *.sym files
        libseqtrace_path=$(grep -h "\.libseqtrace-subprocess.so" ${python_dir}/set-0/*.sym | head -n 1 | awk '{print substr($NF, 1, length($NF)-1)}')
        libseqtrace_new_path="${python_dir}/.libseqtrace-subprocess.so"
        sed -i "s|${libseqtrace_path}|${libseqtrace_new_path}|g" ${python_dir}/set-0/*.sym
        # Generate python trace
        python_prv="${python_output_dir}/${hostId}_python_trace.prv"
        mpi2prv "${python_mpits}" "${python_prv}" "${num_merge_procs}"
      fi
    else
      echo "Python trace information not found" 1>&2
    fi

    # DEAL WITH R TRACE
    R_dir="${tmp_dir}/R"

    missing_mpits=""
    if [ -d "${R_dir}" ]; then
      R_mpits="${R_dir}/TRACE.mpits"
      sed -i "s|//|/|g" "${R_mpits}"
      if [ -f "${R_mpits}" ]; then
        # rm "${R_mpits}"
        # touch "${R_mpits}"
        local original_absolute_path=""
        for f in $(tar -tzf ${package} | grep "\/R\/" | grep .mpit | grep -v mpits); do
          f=$(echo $f | cut -c2-)
          echo "mpit file ${f} not included in the original mpits file. Adding it..." 1>&2
          if [ "${missing_mpits}" == "" ]; then
            missing_mpits="${tmp_dir}${f}"
          else
            missing_mpits="${missing_mpits} -- ${tmp_dir}${f}"
          fi
        done
        R_prv="${R_output_dir}/${hostId}_R_trace.prv"
        mpi2prv_args="${missing_mpits} -o ${R_prv}"
        echo ${mpi2prv_args} | xargs "${mpi2prv_bin}"
      fi
    else
      echo "R trace information not found" 1>&2
    fi

    rm -rf "${tmp_dir}"
  done

  mpi2prv "${mpits}" "${prv}" "${num_merge_procs}"
  endCode=$?
  # cleaning
  rm -rf "${mpits}" "${output_dir}/TRACE.sym"
  rm -rf ${output_dir}/set-*
}

#-------------------------------------
# Merges the events within python traces into the main one
# Parameters:
# 1: directory where to find the main trace
# 2: name of the main prv file
# >2: list of python traces to join into the main
#-------------------------------------
merge_python_traces() {
  check_genPRV_env

  local out_dir=${1}
  local trace_name=${2}
  shift 2
  local python_traces=${*}

  if [ -n "${python_traces}" ]; then
    ${JAVA} \
      -cp "${COMPSS_HOME}/Tools/tracing/compss-tracing.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
      "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/TraceMerging-log4j.${gen_tracing_log_level}" \
      "-Dcompss.trace.logDir=${gen_tracing_log_dir}" \
      es.bsc.compss.tracing.PythonTraceMerger \
      "${out_dir}" "${trace_name}" ${python_traces}
    endCode=$?
  else
    endCode=0
  fi
}

#-------------------------------------
# Merges the events within python traces into the main one
# Parameters:
# 1: directory where to find the main trace
# 2: name of the main prv file
# >2: list of R traces to join into the main
#-------------------------------------
merge_R_traces() {
  check_genPRV_env

  local out_dir=${1}
  local trace_name=${2}
  shift 2
  local R_traces=${*}

  if [ -n "${R_traces}" ]; then
    ${JAVA} \
      -cp "${COMPSS_HOME}/Tools/tracing/compss-tracing.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
      "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/TraceMerging-log4j.${gen_tracing_log_level}" \
      "-Dcompss.trace.logDir=${gen_tracing_log_dir}" \
      es.bsc.compss.tracing.RTraceMerger \
      "${out_dir}" "${trace_name}" ${R_traces}
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
  check_genPRV_env

  local out_dir=${1}
  local trace_name=${2}

  ${JAVA} \
    -cp "${COMPSS_HOME}/Tools/tracing/compss-tracing.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
    "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/TraceMerging-log4j.${gen_tracing_log_level}" \
    "-Dcompss.trace.logDir=${gen_tracing_log_dir}" \
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
  check_genPRV_env

  local out_dir=${1}
  local trace_name=${2}
  shift 2
  local agent_traces=${*}

  ${JAVA} \
    -cp "${COMPSS_HOME}/Tools/tracing/compss-tracing.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
    "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/TraceMerging-log4j.${gen_tracing_log_level}" \
    "-Dcompss.trace.logDir=${gen_tracing_log_dir}" \
    es.bsc.compss.tracing.AgentTraceMerger \
    "${out_dir}" "${trace_name}" ${agent_traces}
  endCode=$?
}

LOADED_SYSTEM_TRACE_GENPRV=1
