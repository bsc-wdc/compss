#!/bin/bash -e

#
# FUNCTIONS
#

clean_unittests() {
  echo "[INFO] Cleaning unittests"
  # Remove temporary files
  # Reestablish dds dataset
  rm pycompss/tests/unittests/dds/dataset/pickles/00001
  rm pycompss/tests/unittests/dds/dataset/tmp/00001
}

clean_integration_unittests() {
  echo "[INFO] Cleaning integration unittests"
  # Remove temporary files
  [ -e outfile_1 ] && rm outfile_1
  [ -e outfile_2 ] && rm outfile_2
  rm compss*.err
  rm compss*.out
}

# Current script directory
CURRENT_DIR="$(pwd)"
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Coverage configuration
COVERAGE_PROCESS_START=${SCRIPT_DIR}/coverage.cfg

# Run directory (where pycompss folder is)
RUN_DIR="${SCRIPT_DIR}/../../src/"

# Prepare PYTHONPATH
export PYTHONPATH=${RUN_DIR}:${PYTHONPATH}

#---------------------------------------------------
# SCRIPT CONSTANTS DECLARATION
#---------------------------------------------------
DEFAULT_UNITTESTS=true
DEFAULT_INTEGRATION_UNITTESTS=true
DEFAULT_JUPYTER_UNITTESTS=true

NO_UNITTESTS="Warning: No unittests specified. Loading default value"
NO_INTEGRATION_UNITTESTS="Warning: No integration unittests specified. Loading default value"
NO_JUPYTER_UNITTESTS="Warning: No jupyter unittests specified. Loading default value"

#---------------------------------------------------
# FUNCTIONS DECLARATION
#---------------------------------------------------
show_opts() {
  cat <<EOT
* Options:
    --help, -h                      Print this help message

    --opts                          Show available options

    --unittests, -u                 Enable Monitor installation
    --no-unittests, -U              Disable Monitor installation
                                    Default: ${DEFAULT_UNITTESTS}
    --integration-unittests, -i     Enable Monitor installation
    --no-integration-unittests, -I  Disable Monitor installation
                                    Default: ${DEFAULT_INTEGRATION_UNITTESTS}
    --jupyter-unittests, -j         Enable Monitor installation
    --no-jupyter-unittests, -J      Disable Monitor installation
                                    Default: ${DEFAULT_JUPYTER_UNITTESTS}

EOT
}

usage() {
  exitValue=$1
  cat <<EOT
Usage: $0 [options]
EOT
  show_opts
  exit "$exitValue"
}

# Displays arguments warnings
display_warning() {
  local warn_msg=$1
  echo "$warn_msg"
}

# Displays parsing arguments errors
display_error() {
  local error_msg=$1
  echo "$error_msg"
  echo " "
  usage 1
}

get_args() {
  # Parse COMPSs' Binding Options
  while getopts hvmMbBpPtTaAkKjJN-: flag; do
    # Treat the argument
    case "$flag" in
      h)
        # Display help
        usage 0
        ;;
      u)
        # Custom unittests value
        unittests=true
        ;;
      U)
        # Custom unittests value
        unittests=false
        ;;
      i)
        # Custom unittests value
        integration_unittests=true
        ;;
      I)
        # Custom unittests value
        integration_unittests=false
        ;;
      j)
        # Custom unittests value
        jupyter_unittests=true
        ;;
      J)
        # Custom unittests value
        jupyter_unittests=false
        ;;
      -)
        # Check more complex arguments
        case "$OPTARG" in
          help)
            # Display help
            usage 0
            ;;
          opts)
            # Display help
            show_opts
            exit 0
            ;;
          unittests)
            # Custom unittests value
            unittests=true
            ;;
          no-unittests)
            # Custom unittests value
            unittests=false
            ;;
          integration-unittests)
            # Custom integration unittests value
            integration_unittests=true
            ;;
          no-integration-unittests)
            # Custom integration unittests value
            integration_unittests=false
            ;;
          jupyter-unittests)
            # Custom jupyter unittests value
            jupyter_unittests=true
            ;;
          no-jupyter-unittests)
            # Custom jupyter unittests value
            jupyter_unittests=false
            ;;
          *)
            # Flag didn't match any patern. End of Unittests flags
            display_error "${INCORRECT_PARAMETER}"
            break
            ;;
        esac
        ;;
      *)
        # Flag didn't match any patern. End of COMPSs flags
        display_error "${INCORRECT_PARAMETER}"
        break
        ;;
    esac
  done
  # Shift option arguments
  shift $((OPTIND-1))
}

check_args() {
  if [ -z "$unittests" ]; then
    display_warning "${NO_UNITTESTS}"
    unittests=${DEFAULT_UNITTESTS}
  fi
  if [ -z "$integration_unittests" ]; then
    display_warning "${NO_INTEGRATION_UNITTESTS}"
    integration_unittests=${DEFAULT_INTEGRATION_UNITTESTS}
  fi
  if [ -z "$jupyter_unittests" ]; then
    display_warning "${NO_JUPYTER_UNITTESTS}"
    jupyter_unittests=${DEFAULT_JUPYTER_UNITTESTS}
  fi

  # Check values
  if [ "${unittests}" != "true" ]; then
    unittests="false"
  fi
  if [ "${integration_unittests}" != "true" ]; then
    integration_unittests="false"
  fi
  if [ "${jupyter_unittests}" != "true" ]; then
    jupyter_unittests="false"
  fi

  if [ "${unittests}" != "true" ]; then
    if [ "${integration_unittests}" != "true" ]; then
      if [ "${jupyter_unittests}" != "true" ]; then
        display_error "ERROR: Disabled all unittests. Please, enable at least one."
        exit 1
      fi
    fi
  fi
}

log_parameters() {
  echo "PARAMETERS:"
  echo "- Unittests             = ${unittests}"
  echo "- Integration unittests = ${integration_unittests}"
  echo "- Jupyter unittests     = ${jupyter_unittests}"
  sleep 2
}