#!/usr/bin/env bash

#---------------------------------------------------
# SCRIPT GLOBAL CONSTANTS
#---------------------------------------------------

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

#---------------------------------------------------
# SCRIPT GLOBAL HELPERS
#---------------------------------------------------

# shellcheck source=./commons
# shellcheck disable=SC1091
source "${SCRIPT_DIR}"/commons


#---------------------------------------------------
# SCRIPT HELPERS
#---------------------------------------------------

remove_symbolic_links (){
    # Retrieve packages (using commons function)
    # Sets global variable packages_folder
    get_packages_folder "$1"
    # shellcheck disable=SC2154
    echo "Cleaning PyCOMPSs symbolic links from ${packages_folder}"
    rm "${packages_folder}"/pycompss
    rm "${packages_folder}"/compss*.so
    rm "${packages_folder}"/process_affinity*.so
}

#---------------------------------------------------
# MAIN
#---------------------------------------------------

if [ -d "${SCRIPT_DIR}/3" ]; then
    remove_symbolic_links python3
fi

rm -rf dist
rm -rf build
rm -rf target
rm -rf src/pycompss.egg-info
