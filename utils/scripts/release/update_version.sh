#!/bin/bash -e

usage() {
  exitValue=$1

  echo " "
  echo "Usage ./update_trunk_version <previousVersion> <newVersion>"
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
  previousVersion=$1
  newVersion=$2
  echo "Updating COMPSs from ${previousVersion} to ${newVersion}..."

  # Define script variables
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  base_trunk_dir=${SCRIPT_DIR}/../../../

  #---------------------------------------------------------------------
  # Modify all pom files
  echo "- Modifying pom files..."
  cd "${base_trunk_dir}"
  pomFiles=$(find . -name pom.xml)
  for pom in $pomFiles; do
    echo "    $pom"
    sed -i 's/<version>'"${previousVersion}"'<\/version>/<version>'"${newVersion}"'<\/version>/g' "$pom"
    sed -i 's/<compss.version>'"${previousVersion}"'<\/compss.version>/<compss.version>'"${newVersion}"'<\/compss.version>/g' "$pom"
  done
  reducedPomFiles=$(find . -name dependency-reduced-pom.xml)
  for reducedPom in $reducedPomFiles; do
    echo "    $reducedPom"
    sed -i 's/<version>'"${previousVersion}"'<\/version>/<version>'"${newVersion}"'<\/version>/g' "$reducedPom"
  done
  cd "${base_trunk_dir}"

  #---------------------------------------------------------------------
  # Modify runtime version file
  echo "- Modifying runtime version file..."
  versionFile=${base_trunk_dir}/compss/runtime/scripts/system/commons/VERSION
  echo "    $versionFile"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$versionFile"

  #---------------------------------------------------------------------
  # Modify python binding files
  echo "- Modifying python binding files..."
  versionFile=${base_trunk_dir}/compss/programming_model/bindings/python/VERSION.txt
  echo "    $versionFile"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$versionFile"
  cd "${base_trunk_dir}"

  #---------------------------------------------------------------------
  # Modify utils/scripts
  echo "- Modifying utils/scripts files..."
  cd "${base_trunk_dir}/utils/scripts/"
  files=$(grep -R -l "$previousVersion" | cat)
  for file in $files; do
    echo "    $file"
    sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$file"
  done
  cd "${base_trunk_dir}"

  #---------------------------------------------------------------------
  # Modify utils/storage
  echo "- Modifying utils/storage files..."
  cd "${base_trunk_dir}/utils/storage/dummyPSCO/"
  file="pom.xml"
  echo "    $file"
  sed -i 's/<version>'"${previousVersion}"'<\/version>/<version>'"${newVersion}"'<\/version>/g' "$file"
  cd "${base_trunk_dir}"

  #---------------------------------------------------------------------
  # Modify builders version
  echo "- Modifying builders files..."
  buildpackages=${base_trunk_dir}/builders/buildpackages
  echo "    $buildpackages"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$buildpackages"
  buildlocal=${base_trunk_dir}/builders/buildlocal
  echo "    $buildlocal"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$buildlocal"
  scREADME=${base_trunk_dir}/builders/specs/sc/README
  echo "    $scREADME"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$scREADME"
  scScript=${base_trunk_dir}/builders/specs/sc/compile_copy.sh
  echo "    $scScript"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$scScript"
  specsPip=${base_trunk_dir}/builders/specs/pip/README
  echo "    $specsPip"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$specsPip"
  specsPipResources=${base_trunk_dir}/builders/specs/pip/pyCOMPSsResources/VERSION.txt
  echo "    $specsPipResources"
  sed -i 's/'"${previousVersion}"'/'"${newVersion}"'/g' "$specsPipResources"

  #---------------------------------------------------------------------
  # NOTIFY SUCCESS
  echo " "
  echo "FINISHED! All compss version updated from ${previousVersion} to ${newVersion}"
  echo " "

  #---------------------------------------------------------------------
  # Warn for RELEASE NOTES and CHANGELOG modifications
  echo "************************************************"
  echo "* WARNING: RELEASE_NOTES and changelog must be *"
  echo "*          manually edited                     *"
  echo "************************************************"

  # END
  exit 0
