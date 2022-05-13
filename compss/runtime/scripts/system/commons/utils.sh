#!/bin/bash

if [ -n "${LOADED_SYSTEM_COMMONS_UTILS}" ]; then
  return 0
fi

# shellcheck source=./logger.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"

#---------------------------------------------------
# ERROR CONSTANTS DECLARATION
#---------------------------------------------------
ERROR_TMP_FILE="Cannot create TMP Submit file"

###############################################
# Function to create a TMP submit script
# WARN: result script left in global TMP_SUBMIT_SCRIPT variable
###############################################
create_tmp_submit() {
  # Create TMP DIR for submit script
  TMP_SUBMIT_SCRIPT=$(mktemp)
  ev=$?
  if [ $ev -ne 0 ]; then
    fatal_error "${ERROR_TMP_FILE}" $ev
  fi

  cat > "${TMP_SUBMIT_SCRIPT}" << EOT
#!/bin/bash -e
#
EOT
}

###############################################
# Infers the language from the Application Path
# WARN: Sets global lang variable
###############################################
infer_language() {
  local fullApp
  local default_language

  local fileName
  local extension
  local isPython
  local isPythonCompiled
  local isC

  fullApp=${1}
  default_language=${2}

  fileName=$(basename "$fullApp")
  extension=${fileName##*.}
  if [ "$extension" == "py" ] || [ "$extension" == "pyc" ]; then
    display_info "Inferred PYTHON language"
    lang=python
  else
    # Try to infer language by file command
    isPython=$(file "$fullAppPath" | grep "Python" | cat)
    isPythonCompiled=$(file "$fullAppPath" | grep "python" | cat)
    isC=$(file "$fullAppPath" | grep -E "executable|ELF" | cat)
    if [ -n "$isPython" ] || [ -n "$isPythonCompiled" ]; then
      display_info "Inferred PYTHON language"
      lang=python
    elif [ -n "$isC" ]; then
      display_info "Inferred C/C++ language"
      lang=c
    else
      # Lang cannot be infered or it's the default JAVA
      display_info "Using default language: ${default_language}"
      lang=${default_language}
    fi
  fi
}


###############################################
# Converts a list of paths into a list of absolute paths (elements separated by ':')
# WARN: Sets global fcp variable
###############################################
convert_pathlist_to_absolute_paths() {
    local cp=${1}
    fcp=""
    for currcp in ${cp//:/$'\n'}; do
      if [ -d "$currcp" ] || [ -f "$currcp" ]; then   # If the dir/file exists locally
        if [ ! "${currcp:0:1}" == '/' ]; then # Relative paths to absolute
          get_path_info "$currcp"
          currcp="${absdir}/${file}"
        fi
      else
	      display_warning "Path \"${currcp}\" does not exist."
        if [ ! "${currcp:0:1}" == '/' ]; then
          currcp=""
        fi
	    fi
      
      if [ -n "${currcp}" ]; then
        if [ -z "${fcp}" ]; then 
          fcp="$currcp"
        else
          fcp="${fcp}:$currcp"
        fi
      fi
    done
}

get_path_info() {
   local fp=${1}
   absdir="$(cd "$(dirname "${fp}")" && pwd)" # Get absolute dir
	 file="$(basename "$fp")"
}

# Checks if a command exists
command_exists () {
  type "$1" &> /dev/null ;
}

###############################################
# Gets an Universally Unique Identifier
# WARN: Sets global uuid variable
###############################################
get_uuid() {
  uuid=$(uuidgen)
  if [ -z "$uuid" ]; then
    uuid=$(cat /proc/sys/kernel/random/uuid)
  fi
}

LOADED_SYSTEM_COMMONS_UTILS=1