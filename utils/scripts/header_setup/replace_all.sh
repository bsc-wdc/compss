#!/bin/bash


#
# SCRIPT CONSTANTS
#
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
INSPECTED_DIRS="compss performance_analysis observability utils/storage"
IGNORE_DIRS="programming_model/bindings/RCOMPSs"

#
# HELPER METHODS
#

change_headers() {
  local find_excludes=()

  for ignore_dir in "${IGNORE_DIRS[@]}"; do
    find_excludes+=(-path "./${ignore_dir}" -prune -o)
  done

  # Add java headers
  find . "${find_excludes[@]}" -name "*.java" -type f -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;

  # Add c headers
  find . "${find_excludes[@]}" -name "*.c" -type f -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;
  find . "${find_excludes[@]}" -name "*.cc" -type f -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;
  find . "${find_excludes[@]}" -name "*.h" -type f -exec "${SCRIPT_DIR}/replace_header.sh" {} java_c \;
  find . "${find_excludes[@]}" -name "Makefile*" -type f -exec "${SCRIPT_DIR}/replace_header.sh" {} python \;

  # Add python headers
  find . "${find_excludes[@]}" -name "*.py" -type f -exec "${SCRIPT_DIR}/replace_header.sh" {} python \;

  # Update placeholder
  "${SCRIPT_DIR}/replace_header.sh" "${SCRIPT_DIR}/copyright_placeholder.txt" "python"
}


#
# MAIN METHOD
#

main() {
  echo "[INFO] Updating headers..."

  for inspect_dir in ${INSPECTED_DIRS}; do
    echo "[INFO] Updating headers on ${inspect_dir}..."
    cd "${SCRIPT_DIR}/../../../${inspect_dir}" || exit 1
    change_headers
    cd "${SCRIPT_DIR}" || exit 1
    echo "[INFO] Headers updated on ${inspect_dir}"
  done

  echo "DONE"
}


#
# ENTRY POINT
#

main
