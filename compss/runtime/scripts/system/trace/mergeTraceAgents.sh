#!/bin/bash

usage(){
    cat <<EOT

  $0 <options>

    options:                
            -h/--help                                       shows this message

            --output_dir=<output_dir>                       the directory where to store the merged traces

            -f/--force_override                             overrides output_dir if it already exists without asking

            --result_trace_name=<result_trace_name>         the name of the generated trace

            --input_dirs=<input_dirs>                       the directories containing the trace folders to merge
                                                            as a lists of paths separated by ';'


EOT
    exit 0
}

# Check COMPSs HOME 
if [ -z "$COMPSS_HOME" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    COMPSS_HOME=${SCRIPT_DIR}/../../../
else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/user"
fi

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

get_args() {
    echo parsing argument
    # Parse COMPSs Options
    while getopts hfon-: flag; do
    echo argument detected
    # Treat the argument
    case "$flag" in
        h)
            # Display help
            usage
        ;;
        f)
            force_override=true
        ;;
        -)
            # Check more complex arguments
            echo complex argument
            case "$OPTARG" in
                help)
                    # Display help
                    usage
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
                input_dirs=*)
                    input_dirs=${OPTARG//input_dirs=/}
                    inputDirsArray=(${input_dirs//;/ })
                ;;
                *)
                    # Flag didn't match any patern. Raise exception
                    arguments_error "Bad argument: $OPTARG"
                ;;
            esac
	;;
      *)
	# Flag didn't match any patern. End of COMPSs flags
	break
	;;
    esac
  done
}


get_args "$@"

if [ -z $output_dir ]; then
    echo "WARNING: output_dir is not set, using default output directory: ${COMPSS_HOME}/agentTraceMerge"
    output_dir="${COMPSS_HOME}/agentTraceMerge"
fi

if [ -z $result_trace_name ]; then
    echo "WARNING: result_trace_name is not set, using default name: \"compss_execution\""
    result_trace_name="compss_execution"
fi

output_dir=$(eval "readlink -f ${output_dir}")
if [ -d  ${output_dir} ]; then
    if [ $force_override ]; then
        echo "WARNING: output folder already exists and will be overriden"
    else
        echo "WARNING: output folder already exists, do you want to overrite it? (y/n)"
        read response
        if [ $response != "y" ] && [ $response != "Y" ] && [ $response != "yes" ] && [ $response != "YES" ]; then
            exit 1
        fi
    fi
fi

for input in "${inputDirsArray[@]}"; do
    echo parsing input $input
    if [ ! -d $input ]; then
        echo "ERROR: input dir $input doesn't exists"
        exit 1
    fi
    if [ ! -d ${input}/trace ]; then
        echo "ERROR: trace dir ${input}/trace doesn't exists"
        exit 1
    fi
    traceFile=${input}/trace/*.prv
    if [ ! -f ${traceFile} ]; then
        echo "ERROR: there's no trace file in directory ${input}/trace"
        exit 1
    fi
done




${JAVA} \
    "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/AgentMerger-log4j" \
    -cp "${COMPSS_HOME}/Runtime/compss-agent-impl.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
    es.bsc.compss.agent.AgentTraceMerger $result_trace_name $output_dir ${inputDirsArray[@]}
    