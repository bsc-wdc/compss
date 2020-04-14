source "${COMPSS_HOME}Runtime/scripts/system/commons/logger.sh"

###############################################
###############################################
#            CONSTANTS DEFINITION
###############################################
###############################################

#----------------------------------------------
# DEFAULT VALUES
#----------------------------------------------

# Master contact details
DEFAULT_MASTER_PORT="[43000,44000]"
DEFAULT_MASTER_NAME=""

# Available Adaptors
NIO_ADAPTOR=es.bsc.compss.nio.master.NIOAdaptor
GAT_ADAPTOR=es.bsc.compss.gat.master.GATAdaptor
REST_AGENT_ADAPTOR=es.bsc.compss.agent.rest.Adaptor
COMM_AGENT_ADAPTOR=es.bsc.compss.agent.comm.CommAgentAdaptor

DEFAULT_COMMUNICATION_ADAPTOR=${NIO_ADAPTOR}
#DEFAULT_COMMUNICATION_ADAPTOR=${GAT_ADAPTOR}

#----------------------------------------------
# ERROR MESSAGES
#----------------------------------------------


###############################################
###############################################
#        ADAPTORS HANDLING FUNCTIONS
###############################################
###############################################
#----------------------------------------------
# CHECK ADAPTORS-RELATED ENV VARIABLES
#----------------------------------------------
check_adaptors_env() {
  # GAT Environment
  if [ -z "${GAT_LOCATION}" ]; then
    GAT_LOCATION=${COMPSS_HOME}/Dependencies/JAVA_GAT
  fi
}


#----------------------------------------------
# CHECK ADAPTORS-RELATED SETUP values
#----------------------------------------------
check_adaptors_setup () {
  # MASTER
  if [ -z "${master_name}" ]; then
    master_name=${DEFAULT_MASTER_NAME}
  fi

  # master_port might be null. In that case, the master tries different ports host the master

  # Adaptor
  if [ -z "$comm" ]; then
    comm=${DEFAULT_COMMUNICATION_ADAPTOR}
  fi
}



#----------------------------------------------
# APPEND PROPERTIES TO FILE
#----------------------------------------------
append_adaptors_jvm_options_to_file() {
  local jvm_options_file=${1}
  cat >> "${jvm_options_file}" << EOT
-Dcompss.comm=${comm}
-Dcompss.masterName=${master_name}
-Dcompss.masterPort=${master_port}
-Dgat.adaptor.path=${GAT_LOCATION}/lib/adaptors
-Dgat.debug=false
-Dgat.broker.adaptor=sshtrilead
-Dgat.file.adaptor=sshtrilead
EOT
}


#----------------------------------------------
# CLEAN ENV
#----------------------------------------------
clean_adaptors_env () {
  : 
}
