#!/bin/bash

# Check COMPSs HOME 
if [ -z "$COMPSS_HOME" ]; then
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    COMPSS_HOME=${SCRIPT_DIR}/../../../
else
    SCRIPT_DIR="${COMPSS_HOME}/Runtime/scripts/user"
fi

# JAVA HOME
  if [[ -z "$JAVA_HOME" ]]; then
    JAVA=java
  elif [ -f "$JAVA_HOME"/jre/bin/java ]; then
    JAVA=$JAVA_HOME/jre/bin/java
  elif [ -f "$JAVA_HOME"/bin/java ]; then
    JAVA=$JAVA_HOME/bin/java
  else
    fatal_error "${JAVA_HOME_ERROR}" 1
  fi


${JAVA} \
    "-Dlog4j.configurationFile=${COMPSS_HOME}/Runtime/configuration/log/AgentMerger-log4j" \
    -cp "${COMPSS_HOME}/Runtime/compss-agent-impl.jar:${COMPSS_HOME}/Runtime/compss-engine.jar" \
    es.bsc.compss.agent.AgentTraceMerger "$@"
