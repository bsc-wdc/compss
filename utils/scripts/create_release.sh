#!/bin/bash -e

########################################################
# Create a new release
########################################################
# Parameters: 
#  - <version_number>: New version number.
#  - <push>          : Commit and push every step.
########################################################
# Variables:
#   base_version: Refers to the current version 
#   new_version : Refers to the version to be created
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
  echo " * Example: ${0} 3.4.post2610 true"
  echo ""
  exit "$exitValue"
}


########################################################
####################### MAIN ###########################
########################################################

if [ $# -ne 2 ]; then
  echo "ERROR: Incorrect number of parameters"
  echo "Parameter 'new_version' or 'push' was not set."
usage 1
fi

# Get new version
new_version=$1

# Get base version
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
base_dir=${SCRIPT_DIR}/../../
base_version=$(cat "${base_dir}/pom.xml" | grep "compss.version" | tr ">" "\t" | awk '{ print $2 }' | tr "<" "\t" | awk '{ print $1 }')

# Get commit changes
commit_changes="false"

echo "================================="
echo "|  Creating new COMPSs release  |"
echo "================================="
echo " - Base version: ${base_version}"
echo " - New version : ${new_version}"
echo " - Commit changes: ${commit_changes}"


########################################################
# Step 1: Check headers (licenses)
########################################################

echo "STEP 1: CHECK HEADERS"
${SCRIPT_DIR}/header_setup/replace_all.sh

########################################################
# Step 2: Update version
########################################################

echo "STEP 2: UPDATE VERSION"
${SCRIPT_DIR}/release/update_version.sh "${base_version}" "${new_version}" "${commit_changes}"

########################################################
# Step 3: Generate tag
########################################################

echo "STEP 3: generate tag"
${SCRIPT_DIR}/release/generate_tag.sh "${new_version}" "${commit_changes}"

########################################################
# Step 4: Artifactory repository update
########################################################

echo "STEP 4: Artifactory repository update"
${SCRIPT_DIR}/release/mvn_deploy_tag.sh "${new_version}" "${commit_changes}"

########################################################
# Step 5: Install in Supercomputers
########################################################
# - Marenostrum GPP
# - Marenostrum ACC
# - Nord

########################################################
# Step 6: Create module files in Supercomputers
########################################################

########################################################
# Step 7: Docker images creation
########################################################

########################################################
# Step 8: Docker repository update
########################################################

########################################################
# Step 9: Email notification
########################################################

echo "=================================="
echo "|  RELEASE SUCCESSFULLY CREATED  |"
echo "=================================="
