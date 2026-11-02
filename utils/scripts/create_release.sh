#!/bin/bash -e

########################################################
# Create a new release
########################################################
# Parameters: 
#  - <new_version>: New version number.
########################################################


########################################################
# SCRIPT CONSTANTS
########################################################

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"


########################################################
# Auxiliar functions
########################################################

usage() {
  exitValue=$1
  echo ""
  echo " * Usage  : ${0} <new_version>"
  echo " * Example: ${0} 4.0.0-alpha.1"
  echo ""
  exit "$exitValue"
}


########################################################
####################### MAIN ###########################
########################################################

if [ $# -ne 1 ]; then
  echo "ERROR: Incorrect number of parameters"
  echo "Parameter 'new_version' was not set."
  usage 1
fi

# Get new version
new_version=$1

# Get base version
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
base_dir=${SCRIPT_DIR}/../../
base_version=$(cat "${base_dir}/pom.xml" | grep "compss.version" | tr ">" "\t" | awk '{ print $2 }' | tr "<" "\t" | awk '{ print $1 }')

echo "================================="
echo "|  Creating new COMPSs release  |"
echo "================================="
echo " - Base version: ${base_version}"
echo " - New version : ${new_version}"


########################################################
# Trigger: Generate tag
########################################################

# .pre----->build->[test]->deploy->.post
# 1->2->3-->4------------->5------>10
#        |->6------------->7-----|
#        `->8------------->9-----'

########################################################
# Step 1: Check headers (licenses)
########################################################

echo "STEP 1: CHECK HEADERS"
#${SCRIPT_DIR}/header_setup/replace_all.sh

########################################################
# Step 2: Update version
########################################################

echo "STEP 2: UPDATE VERSION"
#${SCRIPT_DIR}/release/update_version.sh "${new_version}"

########################################################
# Step 3: Artifactory repository update
########################################################

echo "STEP 3: Artifactory repository update"
#${SCRIPT_DIR}/release/mvn_deploy_tag.sh "${new_version}" true

########################################################
# Step 4: Install in Supercomputers
########################################################
# - Marenostrum GPP
# - Marenostrum ACC
# - Nord

########################################################
# Step 5: Create module files in Supercomputers
########################################################

########################################################
# Step 6: Docker images creation
########################################################

########################################################
# Step 7: Docker repository update
########################################################

########################################################
# Step 8: Build specs
########################################################

########################################################
# Step 9: Upload specs
########################################################

########################################################
# Step 10: Email notification
########################################################

echo "=================================="
echo "|  RELEASE SUCCESSFULLY CREATED  |"
echo "=================================="
