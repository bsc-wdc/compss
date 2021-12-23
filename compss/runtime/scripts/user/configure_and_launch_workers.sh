if [ -z "${COMPSS_SC_CFG}" ]; then
	COMPSS_SC_CFG=default.cfg
fi
source ${COMPSS_HOME}/Runtime/scripts/queues/supercomputers/${COMPSS_SC_CFG}
source ${COMPSS_HOME}/Runtime/scripts/queues/queue_systems/${QUEUE_SYSTEM}.cfg

if [ "${HOSTLIST_CMD}" == "nodes.sh" ]; then
    source "${SCRIPT_DIR}/../../system/${HOSTLIST_CMD}"
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

${COMPSS_HOME}/Runtime/scripts/user/launch_compss --log_level=${COMPS_LOG_LEVEL} --master_node=${master_node} --worker_nodes="${worker_nodes}" --sc_cfg=default.cfg --command $@
