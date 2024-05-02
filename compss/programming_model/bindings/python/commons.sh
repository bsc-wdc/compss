#!/usr/bin/env bash

#---------------------------------------------------
# COMMON HELPER FUNCTIONS
#---------------------------------------------------

get_packages_folder(){
   # Check the packages folder of a particular python interpreter
   # $1 -> python command
   packages_folder=$( $1 -c "import site
if hasattr(site, 'getsitepackages'):
    # Normal execution
    import os
    if os.getuid() == 0:
        # Sudo installation
        packages = site.getsitepackages()[0]
        if isinstance(packages, list):
            print(packages[0])
        else:
            print(packages)
    else:
        # User installation
        packages = site.getusersitepackages()
        if isinstance(packages, list):
            print(packages[0])
        else:
            print(packages)
else:
    # Workaround for virtualenv
    from distutils.sysconfig import get_python_lib
    print([get_python_lib()][0])
" )
}


create_symbolic_links (){
  # Setup the appropriate symbolic links to site-packages/dist-packages
  # $1 -> python command
  # $2 -> origin path

  command=$1
  origin=$2

  echo "Looking for package where to place the symbolic links."
  get_packages_folder "${command}"

  echo "Checking if the folder exists."
  if [ ! -d "${packages_folder}" ]; then
      echo "Could not find folder: ${packages_folder} - Creating it."
      mkdir -p "${packages_folder}"
  fi

  # Setup a symbolic link to pycompss
  ln -sfn "${origin}/pycompss" "${packages_folder}/pycompss"
  ev=$?
  if [ $ev -ne 0 ]; then
    echo "Failed, to create symbolic link from ${origin}/pycompss to ${packages_folder}/pycompss"
    exit $ev
  else
    echo "Created symbolic link from ${origin}/pycompss to ${packages_folder}/pycompss"
  fi

  # Setup a symbolic link to compss module
  ln -sfn "${origin}"/compss.* "${packages_folder}/"
  ev=$?
  if [ $ev -ne 0 ]; then
    echo "Failed, to create symbolic link from ${origin}/compss.* to ${packages_folder}/compss.*"
    exit $ev
  else
    echo "Created symbolic link from ${origin}/compss.* to ${packages_folder}/compss.*"
  fi

  # Setup a symbolic link to thread affinity module
  ln -sfn "${origin}"/process_affinity.* "${packages_folder}/"
  ev=$?
  if [ $ev -ne 0 ]; then
    echo "Failed, to create symbolic link from ${origin}/process_affinity.* to ${packages_folder}/process_affinity.*"
    exit $ev
  else
    echo "Created symbolic link from ${origin}/process_affinity.* to ${packages_folder}/process_affinity.*"
  fi

  # Setup a symbolic link to ear module
  ln -sfn "${origin}"/ear.* "${packages_folder}/"
  ev=$?
  if [ $ev -ne 0 ]; then
    echo "WARNING: Could not create symbolic link from ${origin}/ear.* to ${packages_folder}/ear.*"
  else
    echo "Created symbolic link from ${origin}/ear.* to ${packages_folder}/ear.*"
  fi
}
