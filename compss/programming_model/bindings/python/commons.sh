#!/usr/bin/env bash

#---------------------------------------------------
# COMMON HELPER FUNCTIONS
#---------------------------------------------------

get_packages_folder(){
   # Check the packages folder of a particular python interpreter
   # $1 -> python command
   packages_folder=$( $1 -c "import site, sys, os
def _in_virtualenv():
    return (hasattr(sys, 'real_prefix') or
            (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix))

if hasattr(site, 'getsitepackages'):
    if _in_virtualenv() or os.getuid() == 0:
        # Inside a virtualenv getsitepackages() returns the venv-local path;
        # also use it for root installs to target system site-packages.
        packages = site.getsitepackages()
        if isinstance(packages, list):
            print(packages[0])
        else:
            print(packages)
    else:
        # Non-root system Python: install into the user site-packages.
        packages = site.getusersitepackages()
        if isinstance(packages, list):
            print(packages[0])
        else:
            print(packages)
else:
    # Workaround for very old virtualenv environments (pre-20.x).
    # distutils was removed in Python 3.12, hence the fallback.
    try:
        from distutils.sysconfig import get_python_lib
        print(get_python_lib())
    except ImportError:
        ver = '%d.%d' % sys.version_info[:2]
        print(sys.prefix + '/lib/python' + ver + '/site-packages')
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
