#!/bin/bash -e

usage() {
  exitValue=$1

  echo " "
  echo "Usage: ${BASH_SOURCE[0]} <newVersion>"
  echo " "

  exit "$exitValue"
}

  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
  #=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-
  # Check parameters
  if [ $# -ne 1 ]; then
    echo "ERROR: Incorrect number of parameters"
    usage 1
  fi

  # Define script variables
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  base_dir=${SCRIPT_DIR}/../../../

  # Get versions
  oldVersion=$(cat "${base_dir}/pom.xml" | grep "compss.version" | tr ">" "\t" | awk '{ print $2 }' | tr "<" "\t" | awk '{ print $1 }')
  newVersion=$1
  echo "Updating COMPSs from ${oldVersion} to ${newVersion}..."

  #---------------------------------------------------------------------
  # Modify all pom files
  echo "- Modifying pom files..."
  cd "${base_dir}"
  pomFiles=$(find . -name pom.xml)
  for pom in $pomFiles; do
    echo "    $pom"
    sed -i 's/<version>'"${oldVersion}"'<\/version>/<version>'"${newVersion}"'<\/version>/g' "$pom"
    sed -i 's/<compss.version>'"${oldVersion}"'<\/compss.version>/<compss.version>'"${newVersion}"'<\/compss.version>/g' "$pom"
  done
  reducedPomFiles=$(find . -name dependency-reduced-pom.xml)
  for reducedPom in $reducedPomFiles; do
    echo "    $reducedPom"
    sed -i 's/<version>'"${oldVersion}"'<\/version>/<version>'"${newVersion}"'<\/version>/g' "$reducedPom"
  done
  cd "${base_dir}"

  #---------------------------------------------------------------------
  # Modify runtime version file
  echo "- Modifying runtime version file..."
  versionFile=${base_dir}/compss/runtime/scripts/system/commons/VERSION
  echo "    $versionFile"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$versionFile"

  #---------------------------------------------------------------------
  # Modify python binding files
  echo "- Modifying python binding files..."
  versionFile=${base_dir}/compss/programming_model/bindings/python/VERSION.txt
  echo "    $versionFile"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$versionFile"
  cd "${base_dir}"

  #---------------------------------------------------------------------
  # Modify utils/scripts
  echo "- Modifying utils/scripts files..."
  cd "${base_dir}/utils/scripts/"
  files=$(grep -R -l "$oldVersion" | cat)
  for file in $files; do
    echo "    $file"
    sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$file"
  done
  cd "${base_dir}"

  #---------------------------------------------------------------------
  # Modify utils/storage
  echo "- Modifying utils/storage files..."
  cd "${base_dir}/utils/storage/dummyPSCO/"
  file="pom.xml"
  echo "    $file"
  sed -i 's/<version>'"${oldVersion}"'<\/version>/<version>'"${newVersion}"'<\/version>/g' "$file"
  cd "${base_dir}"

  #---------------------------------------------------------------------
  # Modify builders version
  echo "- Modifying builders files..."
  buildpackages=${base_dir}/builders/buildpackages
  echo "    $buildpackages"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$buildpackages"
  buildlocal=${base_dir}/builders/buildlocal
  echo "    $buildlocal"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$buildlocal"
  scREADME=${base_dir}/builders/specs/sc/README
  echo "    $scREADME"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$scREADME"
  scScript=${base_dir}/builders/specs/sc/compile_copy.sh
  echo "    $scScript"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$scScript"
  specsPip=${base_dir}/builders/specs/pip/README
  echo "    $specsPip"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$specsPip"
  specsPipResources=${base_dir}/builders/specs/pip/pyCOMPSsResources/VERSION.txt
  echo "    $specsPipResources"
  sed -i 's/'"${oldVersion}"'/'"${newVersion}"'/g' "$specsPipResources"

  #---------------------------------------------------------------------
  # NOTIFY SUCCESS
  echo " "
  echo "FINISHED! All compss version updated from ${oldVersion} to ${newVersion}"
  echo " "

  #---------------------------------------------------------------------
  # Warn for RELEASE NOTES and CHANGELOG modifications
  echo "**********************************************"
  echo "* WARNING: RELEASE_NOTES.md and CHANGELOG.md *"
  echo "*          must be manually edited           *"
  echo "**********************************************"

  # END
  exit 0
