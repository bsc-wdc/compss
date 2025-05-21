#!/usr/bin/env bash

#####################################################################
# Name:         install.sh
# Description:  COMPSs' Python binding building script.
# Parameters:
#		[--unittests]                 Enable unittests
#		[--no-unittests]              Disable unittests
#		target_dir                    Target directory where to install the python binding
#		create_symlinks               Create symbolic links within site/dist-packages folders (true or false)
#		specific_python_command       Use specific python command (usually python3)
#		compile                       Compile the installation (true or false)
######################################################################



######################################################################

#---------------------------------------------------
# SCRIPT CONSTANTS DECLARATION
#---------------------------------------------------
DEFAULT_UNITTESTS=false
DEFAULT_CREATE_SYMLINKS=true
DEFAULT_SPECIFIC_PYTHON_COMMAND=python3
DEFAULT_COMPILE=false

INCORRECT_PARAMETER="Error: No such parameter"
INCORRECT_TARGET_DIR="Error: No target directory"
NO_UNITTESTS="Warning: No unittests specified. Loading default value"

SETUPTOOLS_VERSION="61.0.0"

#---------------------------------------------------
# SET SCRIPT VARIABLES
#---------------------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BINDING_DIR="$( dirname "${SCRIPT_DIR}")"
export BINDING_DIR  # used from setup.py

# shellcheck source=./commons.sh
# shellcheck disable=SC1091
source "${SCRIPT_DIR}"/commons.sh


#---------------------------------------------------
# FUNCTIONS DECLARATION
#---------------------------------------------------
show_opts() {
  cat <<EOT
* Options:
    --help, -h                  Print this help message

    --opts                      Show available options

    --unittests, -u             Enable Monitor installation
    --no-unittests, -U          Disable Monitor installation
                                Default: ${DEFAULT_UNITTESTS}

* Parameters:
    target_dir                  COMPSs' Python Binding installation directory
    create_symlinks             Create symbolic links within site/dist-packages folders (true or false)
                                Default: ${DEFAULT_CREATE_SYMLINKS}
    specific_python_command     Use specific python command (usually python3)
                                Default: ${DEFAULT_SPECIFIC_PYTHON_COMMAND}
    compile                     Compile the installation (true or false)
                                Default: ${DEFAULT_COMPILE}

EOT
}

usage() {
  exitValue=$1

  cat <<EOT
Usage: $0 [options] target_dir [create_symlinks] [specific_python_command] [compile]
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
          *)
            # Flag didn't match any pattern. End of COMPSs' Python Binding flags
            display_error "${INCORRECT_PARAMETER}"
            break
            ;;
        esac
        ;;
      *)
        # Flag didn't match any pattern. End of COMPSs flags
        display_error "${INCORRECT_PARAMETER}"
        break
        ;;
    esac
  done
  # Shift option arguments
  shift $((OPTIND-1))

  # Parse target directory location
  if [ $# -gt 0 ]; then
    target_dir=$1
  else
    display_error "${INCORRECT_TARGET_DIR}"
    exit 1
  fi
  shift 1
  if [ $# -ne 0 ]; then
    if [ $# -eq 1 ]; then
      create_symlinks=$1
      specific_python_command=${DEFAULT_SPECIFIC_PYTHON_COMMAND}
      compile=${DEFAULT_COMPILE}
    fi
    if [ $# -eq 2 ]; then
      create_symlinks=$1
      specific_python_command=$2
      compile=${DEFAULT_COMPILE}
    fi
    if [ $# -eq 3 ]; then
      create_symlinks=$1
      specific_python_command=$2
      compile=$3
    fi
  else
    create_symlinks=${DEFAULT_CREATE_SYMLINKS}
    specific_python_command=${DEFAULT_SPECIFIC_PYTHON_COMMAND}
    compile=${DEFAULT_COMPILE}
  fi

  # Check if create symlinks and compile
  if [ "${create_symlinks}" != "true" ]; then
    create_symlinks="false"
  fi
  if [ "${compile}" != "true" ]; then
    compile="false"
  fi
}

check_args() {
  if [ -z "$unittests" ]; then
    display_warning "${NO_UNITTESTS}"
    unittests=${DEFAULT_UNITTESTS}
  fi
}

log_parameters() {
  echo "PARAMETERS:"
  echo "- Unittests        = ${unittests}"
  echo "- Target directory = ${target_dir}"
  echo "- Create symlinks  = ${create_symlinks}"
  echo "- Python command   = ${specific_python_command}"
  echo "- Compile          = ${compile}"
  sleep 5
}

#---------------------------------------------------
# HELPER FUNCTIONS
#---------------------------------------------------

command_exists () {
  type "$1" &> /dev/null ;
}

clean() {
  rm -rf "${SCRIPT_DIR}"/build
  rm -rf "${SCRIPT_DIR}"/target
}

compare_versions() {
  # Returns 1 if $1 >= $2
  #         0 else if $1 < $2
  ver1="$1"
  ver2="$2"

  # Split the versions in parts
  parts1=($(echo "$ver1" | tr '.' ' '))
  parts2=($(echo "$ver2" | tr '.' ' '))

  for i in "${arr[@]}"
  do
    echo "$i"
  done

  # Compare all parts
  for (( i = 0; i < ${#parts1[@]}; i++ )); do
    if [[ "${parts1[$i]}" -eq "${parts2[$i]}" ]]; then
      #echo "- ${#parts1[$i]} is equal than ${#parts2[$i]}"
      echo ""
    elif [[ "${parts1[$i]}" -gt "${parts2[$i]}" ]]; then
      #echo "- ${#parts1[$i]} is higher than ${#parts2[$i]}"
      echo "1"
      return
    elif [[ "${parts1[$i]}" -lt "${parts2[$i]}" ]]; then
      #echo "- ${#parts1[$i]} is lower than ${#parts2[$i]}"
      echo "0"
      return
    fi
  done

  echo "1"

  # If all are equal:
  # echo "ver1 is equal as ver2"
  return
}

install () {
  local python_command=$1
  local target_directory=$2
  local compile=$3
  local python_complete_version
  local python_major_version

  python_complete_version=$(${python_command} -V 2>&1 | tr -d '\n')
  python_major_version=$(echo "${python_complete_version}" | sed 's/.* \([0-9]\).\([0-9]\).*/\1/' | tr -d '\n')

  pycompss_home="${target_directory}/${python_major_version}"
  export PYTHONPATH=${pycompss_home}:${OLD_PYTHONPATH}

  # Get setuptools and wheel versions
  setuptools_version=$(${python_command} -c "import setuptools; print(setuptools.__version__)")

  echo "INFO: Installation parameters:"
  echo "      - Current script directory: ${SCRIPT_DIR}"
  echo "      - Python command: ${python_command}"
  echo "      - Target directory: ${target_directory}"
  echo "      - PyCOMPSs home: ${pycompss_home}"
  echo "      - Python complete version: ${python_complete_version}"
  echo "      - Python major version: ${python_major_version}"
  echo "      - Python setuptools version: ${setuptools_version}"

  # Check that the sources can be byte-compiled - this avoids syntax errors
  # that are not checked on the installation
  ${python_command} -m compileall "${SCRIPT_DIR}"/src/pycompss
  exitCode=$?
  if [ $exitCode -ne 0 ]; then
    echo "ERROR: Cannot install PyCOMPSs. REASON: Could not byte-compile."
    exit $exitCode
  fi

  # Check that setuptools is or not old
  setuptools_comparison=$(compare_versions "${setuptools_version}" "${SETUPTOOLS_VERSION}" | xargs)

  # Do the installation
  echo "INFO: Starting the installation... Please wait..."
  if [[ ${setuptools_comparison} -eq 1 ]]; then
    ${python_command} -m pip install --no-build-isolation --target="${pycompss_home}" "${SCRIPT_DIR}/."
    # ${python_command} -m pip install --target="${pycompss_home}" "${SCRIPT_DIR}/."
    exitCode=$?
  elif [[ ${setuptools_comparison} -eq 0 ]]; then
    current_dir=$(pwd)
    cd ${SCRIPT_DIR}
    ${python_command} setup.py install --single-version-externally-managed --root="/" --install-lib="${pycompss_home}" -O2
    #${python_command} setup.py install --install-lib="${pycompss_home}" -O2
    cd ${current_dir}
    exitCode=$?
  fi
  if [ $exitCode -ne 0 ]; then
    echo "ERROR: Cannot install PyCOMPSs using ${python_command}"
    exit $exitCode
  fi

  # Extra installation steps only for Mac
  if [ "$(uname)" == "Darwin" ]; then
    binding_py="$( dirname "${target_directory}")"
    echo "INFO: Extra installation steps for Mac..."
    echo "      - binding_py: ${binding_py}"
    install_name_tool -change ${BINDING_DIR}/bindings-common/lib/libbindings_common.0.dylib ${binding_py}/bindings-common/lib/libbindings_common.dylib ${target_directory}/${python_major_version}/compss*.so
  fi

  # Clean unnecessary files
  echo "INFO: Cleaning unnecessary files..."
  if [ -d "${pycompss_home}/__pycache__" ]; then
      rm -rf ${pycompss_home}/__pycache__
  fi

  # Create symbolic links
  if [ "${create_symlinks}" = "true" ]; then
    echo "INFO: Creating symbolic links to site-packages or dist-packages..."
    # Expected when installing with buildlocal. Not from buildsc.
    # Pip package sets these symbolic links and updates the 'activate' script accordingly.
    create_symbolic_links "${python_command}" "${pycompss_home}"
  fi

  # Copy mypy compiling script
  if [ "${python_major_version}" == "3" ]; then
    echo "INFO: Copying compilation script..."
    cp "${SCRIPT_DIR}/scripts/compilation/compile.sh" "${pycompss_home}"
    sed -i "s/cd \"\${SCRIPT\_DIR}\/..\/..\/src\/\"/cd \"\${SCRIPT\_DIR}\"/g" "${pycompss_home}/compile.sh"
    if [ "${compile}" = "true" ]; then
      cd "${pycompss_home}" || exit 1
      ./compile.sh
      ev=$?
      if [ $ev -ne 0 ]; then
        echo "ERROR: Cannot compile with Mypy."
        exit $exitCode
      fi
      cd "${SCRIPT_DIR}" || exit 1
    fi
  fi

  # Copy cleaning and commons scripts for setup or uninstalling
  cp "${SCRIPT_DIR}/commons.sh" "${target_directory}"
  cp "${SCRIPT_DIR}/clean.sh" "${target_directory}"
}


#---------------------------------------------------
# UNIT TESTING FUNCTION
#---------------------------------------------------

run_unittests () {
  if [ "${unittests}" = "true" ]; then
    cd "${SCRIPT_DIR}/scripts/test" || exit 1
    ./run_unittests.sh
    ev=$?
    if [ $ev -ne 0 ]; then
      echo "ERROR: Unittests FAILED! Please check!"
      exit $exitCode
    fi
    cd "${SCRIPT_DIR}" || exit 1
  fi
}

#---------------------------------------------------
# MAIN INSTALLATION FUNCTION
#---------------------------------------------------

install_python_binding () {
  # Add trap for clean
  trap clean EXIT
  TARGET_OS=$(uname)
  export TARGET_OS
  # Install
  OLD_PYTHONPATH=${PYTHONPATH}

  echo "INFO: Starting Python binding installation"

  # Install
  if [ -z "${specific_python_command}" ]; then
    # Install using python3
    if [[ "${target_dir}" = *site-packages* ]] || [[ "${target_dir}" = *dist-packages* ]]; then
      # Check ${target_dir} = site-packages usual, dist-packages in deb based distributions.
      if [[ "${target_dir}" = *python3* ]] || [[ "${target_dir}" = *Python/3* ]]; then
        echo "INFO: Installing within python 3.X site-packages or dist-packages."
        install "python3" "${target_dir}" "${compile}"
      else
        echo "ERROR! Unsupported target directory within site-packages OR dist-packages. Must be Python 3."
        exit 1
      fi
    else
      # Given target dir not in site-packages nor dist-packages
      echo "INFO: Installing within ${target_dir}"
      install "python3" "${target_dir}" "${compile}"
    fi
  else
    # Given a specific python command
    if command_exists "${specific_python_command}" ; then
      echo "INFO: Installing PyCOMPSs for ${specific_python_command}"
      # Check that the python version used is higher than 3.5.
      specific_python_command_complete_version=$(${specific_python_command} -V 2>&1 | tr -d '\n')
      specific_python_collapsed_version=$(echo "${specific_python_command_complete_version}" | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/' | tr -d '\n')
      if [ "${specific_python_collapsed_version}" -lt "30" ]; then
        echo "ERROR: Cannot install PyCOMPSs for Python version: ${specific_python_command_complete_version}"
        echo "       PyCOMPSs requires Python 3.X or greater"
        exit 1
      else
        echo "INFO: Installing using ${specific_python_command} within ${target_dir}"
        install "${specific_python_command}" "${target_dir}" "${compile}"
      fi
    else
      echo "ERROR! ${specific_python_command} IS NOT AVAILABLE."
      exit 1
    fi
  fi
}

#---------------------------------------------------
# MAIN EXECUTION
#---------------------------------------------------
get_args "$@"
check_args
log_parameters
run_unittests
install_python_binding

# END
echo "INFO: SUCCESS: Python binding installed"
# Normal exit
exit 0
