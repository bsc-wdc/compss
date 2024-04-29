#!/bin/bash

if [ -z "${COMPSS_HOME}" ]; then
  SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
  export COMPSS_HOME=${SCRIPT_DIR}/../../../../../
else
  SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/system/adaptors/gos"
fi

### Handles communication of responses for asynchronous behaviour of GOS Adaptor

#Batch task, order of calls
#SUBMIT
  #=> create kill script
#LAUNCH
#RUN
#END

#Interactive task, order of calls
#LAUNCH
  #=> create kill script
#RUN
#END

#$1 Response Dir $2 ResponseFile $3 BatchID
mark_as_submit() {
  local pathDir=$1
  local pathFile=$2
  local id=$3

  mkdir -p "$pathDir"
  echo "[RESPONSE.SH] submit $id in $pathFile";
  echo "$id SUBMIT"> "$pathFile";
}

mark_as_fail() {
  local pathDir=$1
  local pathFile=$2
  local id=$3

  mkdir -p "$pathDir"
  echo "[RESPONSE.SH] fail $id in $pathFile";
  echo "$id FAIL"> "$pathFile";
}

mark_as_cancel() {
  local pathDir=$1
  local pathFile=$2
  local id=$3

  mkdir -p "$pathDir"
  echo "[RESPONSE.SH] fail $id in $pathFile";
  echo "$id CANCEL"> "$pathFile";
}

mark_as_end() {
  #batchID=$(cat $2 | awk '{print $1}')
  local pathDir=$1
  local pathFile=$2
  local id=$3

  mkdir -p "$pathDir"
  echo "[RESPONSE.SH] end $id in $pathFile";
  echo "$id END"> "$pathFile";
}

mark_as_run() {
  local pathDir=$1
  local pathFile=$2
  local id=$3

  mkdir -p "$pathDir"
  echo "[RESPONSE.SH] run $id in $pathFile";

  echo "$id RUN"> "$pathFile";
}

mark_as_launch() {
  #if task is interactive this is the first function called so the responseFile
  #should not exist
  local pathDir=$1
  local pathFile=$2
  local id=$3

  mkdir -p "$pathDir"
  echo "[RESPONSE.SH] launch $id in $pathFile";
  echo "$id LAUNCH"> "$pathFile";
}



create_kill_script_interactive() {
    local killResponseDir=$1
    local killFileScript=$2
    local PID=$3
    mkdir -p "$killResponseDir"
    echo "source $SCRIPT_DIR/response.sh" > "$killFileScript";
    echo "create_empty_files \$@" >> "$killFileScript"
    echo "kill -9 $PID" > "$killFileScript";

}

create_empty_files(){
   for FILE in "$@"; do 
        if [ ! -f "$FILE" ]; then
		echo "Creating empty file $FILE"
		touch "$FILE"
	fi
    done
}

create_kill_script_batch() {
   local killResponseDir=$1
   local killFileScript=$2
   local killCommand=$3
   mkdir -p "$killResponseDir"
   echo "source $SCRIPT_DIR/response.sh" > "$killFileScript";
   echo "create_empty_files \$@" >> "$killFileScript";
   echo "$killCommand" >> "$killFileScript";
   chmod +x "$killFileScript"
}



