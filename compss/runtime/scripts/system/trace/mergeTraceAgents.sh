#!/bin/bash

usage(){
  cat <<EOT
  $0 <options> INPUT_DIR...

    options:                
            -h/--help                                       shows this message

            --output_dir=<output_dir>                       the directory where to store the merged traces

            -f/--force_override                             overrides output_dir if it already exists without asking

            --result_trace_name=<result_trace_name>         the name of the generated trace
EOT
}


get_args() {
    # Parse Script Options
    while getopts hfon-: flag; do
    # Treat the argument
    case "$flag" in
        h)
            # Display help
            usage
            exit 0
        ;;
        f)
            force_override=true
        ;;
        -)
            # Check more complex arguments
            case "$OPTARG" in
                help)
                    # Display help
                    usage
                    exit 1
                ;;
                force_override)
                    force_override=true
                ;;
                output_dir=*)
                    output_dir=${OPTARG//output_dir=/}
                ;;
                result_trace_name=*)
                    result_trace_name=${OPTARG//result_trace_name=/}
                ;;
                *)
                    # Flag didn't match any patern. Raise exception
                    echo "Bad argument: $OPTARG" 1>&2
                    exit 1
                ;;
            esac
	;;
      *)
	# Flag didn't match any patern. End of script flags
	break
	;;
    esac
  done
  shift $((OPTIND-1))
  input_dirs=$*
}


#######################################
# Main script
#######################################

# Check COMPSs HOME 
if [ -z "$COMPSS_HOME" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    COMPSS_HOME=${SCRIPT_DIR}/../../../
else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/user"
fi

CURRENT_DIR=$(pwd)

# JAVA HOME
if [[ -z "$JAVA_HOME" ]]; then
  JAVA=java
elif [ -f "$JAVA_HOME"/jre/bin/java ]; then
  JAVA=$JAVA_HOME/jre/bin/java
elif [ -f "$JAVA_HOME"/bin/java ]; then
  JAVA=$JAVA_HOME/bin/java
else
  fatal_error "${JAVA_HOME_ERROR}" 1
fi

# Parse script options
get_args "$@"

# Check script options
if [ -z "${input_dirs}" ]; then
  echo "Error: Missing input traces to merge" 1>&2
  exit 1
fi


if [ -z "${output_dir}" ]; then
    echo "WARNING: output_dir is not set, using default output directory: ${CURRENT_DIR}/agentTraceMerge" 1>&2
    output_dir="${CURRENT_DIR}/agentTraceMerge"
fi

if [ -z "${result_trace_name}" ]; then
    echo "WARNING: result_trace_name is not set, using default name: \"compss_execution\"" 1>&2
    result_trace_name="compss_execution"
fi

output_dir=$(eval "readlink -f ${output_dir}")
if [ -d  "${output_dir}" ]; then
    if [ ${force_override} ]; then
        echo "WARNING: output folder already exists and will be overriden" 1>&2
    else
        echo "WARNING: output folder already exists, do you want to overrite it? (y/n)" 1>&2
        read response
        if [ "${response}" != "y" ] && [ "${response}" != "Y" ] && [ "${response}" != "yes" ] && [ "${response}" != "YES" ]; then
            exit 1
        fi
    fi
    rm -rf "${output_dir}"
    mkdir -p "${output_dir}"
fi

for input in ${input_dirs}; do
    input=$(eval "readlink -f ${input}")
    if [ ! -d "${input}" ]; then
        echo "ERROR: input dir $input doesn't exists" 1>&2
        exit 1
    fi
    if [ ! -d "${input}/trace" ]; then
        echo "ERROR: trace dir ${input}/trace doesn't exists" 1>&2
        exit 1
    fi

    trace_file=$(ls "${input}/trace/" | grep .prv)
    trace_file="${input}/trace/${trace_file}"
    if [ ! -f "${trace_file}" ]; then
        echo "ERROR: there's no trace file in directory ${input}/trace" 1>&2
        exit 1
    fi
done

# Run merger
${JAVA} \
    "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/AgentMerger-log4j" \
    -cp "${COMPSS_HOME}/Runtime/compss-agent-impl.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
    -Dcompss.appLogDir="${output_dir}" \
    es.bsc.compss.agent.AgentTraceMerger "$result_trace_name" "$output_dir" ${input_dirs}