#!/usr/bin/env bash

#####################################################################
# Name:         build.sh
# Description:  COMPSs' Python binding building script.
######################################################################

# Add trap for clean
TARGET_OS=$(uname)
export TARGET_OS

#---------------------------------------------------
# SET SCRIPT VARIABLES
#---------------------------------------------------
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BINDING_DIR="$( dirname "${SCRIPT_DIR}" )"
# BINDING_DIR="${SCRIPT_DIR}/COMPSs/Bindings/"
export BINDING_DIR

# First, build building-commons
cd ${BINDING_DIR}/bindings-common/
./install_common
cd ${SCRIPT_DIR}

# Second, build package
python3 -m build

# Third, clean

# END
echo "INFO: SUCCESS: Python binding packages created"
# Normal exit
exit 0
