#!/bin/bash -e

usage() {
  exitValue=$1

  echo " "
  echo "Usage ./mvn_deploy_tag <version> <deploy>"
  echo " "

  exit "$exitValue"
}
 
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
#=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
# Check parameters
if [ $# -ne 2 ]; then
  echo "ERROR: Incorrect number of parameters"
  usage 1
fi

# Get version parameters
version=$1
deploy=$2

echo "Building Maven COMPSs Tag ${version} to MVN and deploy (${deploy})..."

# Define script variables
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
trunk_dir=${SCRIPT_DIR}/../../../

#---------------------------------------------------------------------
# Deploy
echo "- Compiling and installing"
cd "${trunk_dir}"
mvn -U clean install
cd "${SCRIPT_DIR}"

if [ "$deploy" == "true" ]; then
  echo "- Deploying mvn"
  cd "${trunk_dir}"
  mvn deploy
  cd "${SCRIPT_DIR}"
fi

#---------------------------------------------------------------------
# NOTIFY SUCCESS
echo " "
echo "FINISHED! COMPSs Tag ${version} deployed to Maven"
echo " "

# END
exit 0

