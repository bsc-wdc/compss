#!/bin/bash

  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

  # Check and get parameters
  if [ $# != 2 ]; then
    echo "Usage: replace_header.sh <file> <language>"
    exit 1
  fi
  file=$1
  lang=$2

  # Change IFS for the generation of the array
  IFS="
"
  # The regular expression recognise copyrights of the form :
  #    copyright date institution
  #    copyright © date institution
  #    copyright (c) date institution
  # The regex is case insensitive. The date can take three formats
  # ("year-year", "year, year" and "year year"). '-' and ',' can be surounded
  # by an arbitrary number of spaces. The different date formats can be mixed.
  # A year consist of at least 2 digits.
  cpyrght_re="copyright\([[:blank:]]*\((c)\|©\)\)\?"
  year_re="[[:digit:]]\{2,\}"
  date_re="${year_re}\(\([[:blank:]]*\(-\|,\)[[:blank:]]*\|[[:blank:]]\{1,\}\)${year_re}\)*"

  # The awk script job is to extract only the first header (if any). It avoids
  # adding institutions entries coming from further copyright blocks.
  cpyrghts=(`awk -f "$SCRIPT_DIR/extract_header_$lang.awk" "$file" |
    grep --no-filename --only-matching --ignore-case -e "${cpyrght_re}[[:blank:]]\{1,\}${date_re}[[:blank:]]\{1,\}.*"`)

  placeholder='Copyright 2002-2024 Barcelona Supercomputing Center (www.bsc.es)'

  # If any copyright have been already added, copy it to be sure none is lost.
  # If no licence has been applied, consider it is a BSC file and add the BSC
  # Apache Licence, version 2.0 copyright.

  # WARNING: If the original licence is not "Apache Licence, Version 2.0", the
  # script will drop it.

  # Replace header
  cat "$SCRIPT_DIR/header.template.top.$lang" > "$SCRIPT_DIR/tmp"
  if [ 0 -eq "${#cpyrghts[@]}" ]; then
    cat "$SCRIPT_DIR/header.template.copyright.$lang" >> "$SCRIPT_DIR/tmp"
  else
    for cpyrght in "${cpyrghts[@]}"; do
      cat "$SCRIPT_DIR/header.template.copyright.$lang" | \
        sed -e "s/$placeholder/$cpyrght/" >> "$SCRIPT_DIR/tmp"
    done
  fi
  cat "$SCRIPT_DIR/header.template.bottom.$lang" >> "$SCRIPT_DIR/tmp"

  awk -f "$SCRIPT_DIR/remove_header_$lang.awk" "$file" >> "$SCRIPT_DIR/tmp"
  mv "$SCRIPT_DIR/tmp" "$file"
