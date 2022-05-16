#!/bin/bash

# Setting up COMPSs_HOME
if [ -z "${COMPSS_HOME}" ]; then
  COMPSS_HOME="$( cd "$( dirname "${BASH_SOURCE[0]}" )"/../../.. && pwd )/"
fi
if [ ! "${COMPSS_HOME: -1}" = "/" ]; then
  COMPSS_HOME="${COMPSS_HOME}/"
fi
export COMPSS_HOME=${COMPSS_HOME}

if [ -z "${COMPSS_SC_CFG}" ]; then
	COMPSS_SC_CFG="${COMPSS_HOME}/Runtime/scripts/queues/supercomputers/default.cfg"
fi

# shellcheck source=../queues/supercomputers/default.sh"
# shellcheck disable=SC1091
source "${COMPSS_SC_CFG}"
# shellcheck source=../queues/queue_systems/slurm.sh"
# shellcheck disable=SC1091
source "${COMPSS_HOME}/Runtime/scripts/queues/queue_systems/${QUEUE_SYSTEM}.cfg"

if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    # shellcheck source=../system/nodes.sh"
    # shellcheck disable=SC1091
    source "${COMPSS_HOME}/Runtime/scripts/system/${HOSTLIST_CMD}"
else
    hosts_cmd="${HOSTLIST_CMD} \$${ENV_VAR_NODE_LIST}${env_var_suffix} ${HOSTLIST_TREATMENT}"
    echo "CMD: $cmd"
    host_list=$(eval ${hosts_cmd})
    echo "host_list: ${host_list}"
    master_cmd="${MASTER_NAME_CMD}"
    master_node=$(eval ${master_cmd})
    worker_nodes=$(echo ${host_list} | sed -e "s/${master_node}//g")
    
    echo "worker_nodes: ${worker_nodes}"
fi

if [ -z "$COMPSS_LOG_LEVEL" ]; then
   export COMPSS_LOG_LEVEL=off
fi

"${COMPSS_HOME}/Runtime/scripts/user/launch_compss" "--log_level=${COMPSS_LOG_LEVEL}" "--master_node=${master_node}" "--worker_nodes=${worker_nodes}" "--sc_cfg=default.cfg" "--command" $@
