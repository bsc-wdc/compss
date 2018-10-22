#!/bin/bash

job_name=$1      # Not supported yet
exec_time=$2     # Walltime in minutes
num_nodes=$3     # Number of nodes
qos=$4           # Quality of service
tracing=$5       # Tracing
storage_home=$6  # Storage home path
storage_props=$7 # Storage properties file


###############################################
#     Get the Supercomputer configuration     #
###############################################
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Load default CFG for default values
DEFAULT_SC_CFG="default"
defaultSC_cfg=${SCRIPT_DIR}/../../queues/cfgs/${DEFAULT_SC_CFG}.cfg
#shellcheck source=../../queues/cfgs/default.cfg
source "${defaultSC_cfg}"
defaultQS_cfg=${SCRIPT_DIR}/../../queues/${QUEUE_SYSTEM}/${QUEUE_SYSTEM}.cfg
#shellcheck source=../../queues/slurm/slurm.cfg
source "${defaultQS_cfg}"


###############################################
#       Submit the jupyter notebook job       #
###############################################
if [ ${storage_home} = "undefined" ]; then
    result=$(enqueue_compss --exec_time=${exec_time} --num_nodes=${num_nodes} --qos=${qos} --tracing=${tracing} --lang=python --jupyter_notebook)
else
    result=$(enqueue_compss --exec_time=${exec_time} --num_nodes=${num_nodes} --qos=${qos} --tracing=${tracing} --storage_home=${storage_home} --storage_props=${storage_props} --lang=python --jupyter_notebook)
fi
submit_line=$(echo "$result" | grep "Submitted")
job_id=(${submit_line//Submitted batch job/ })
if [ -z "$job_id" ]; then
    echo "JobId: FAILED"
    echo $result
    exit 1
fi
echo "JobId: $job_id"

###############################################
#         Wait for the job to start           #
###############################################

function job_is_pending {
    status=$(${QUEUE_JOB_STATUS_CMD} ${job_id})
    if [ ${status} = ${QUEUE_JOB_RUNNING_TAG} ]; then
        false;
    else
        true;
    fi
    return $?;
}

while job_is_pending ; do
    # echo "The job ${job_id} is pending..."
    sleep 5
done
# echo "The job ${job_id} is now running"


###############################################
#            Get the master node              #
###############################################

nodes=$(${QUEUE_JOB_NODES_CMD} ${job_id})
# expanded_nodes=$(scontrol show hostname ${nodes} | paste -d, -s)
expanded_nodes=$(${HOSTLIST_CMD} ${nodes} | paste -d, -s)
nodes_array=(${expanded_nodes//,/ })
master_node=${nodes_array[0]}
worker_nodes=${nodes_array[@]:1}
# echo "Assigned_nodes: $nodes"
# echo "Expanded_assigned_nodes: $expanded_nodes"
echo "MainNode: $master_node"   # Beware, this print is used by pycompss_interactive_sc
# echo "OtherNodes: $worker_nodes"


###############################################
#      Get the Jupyter-notebook token         #
###############################################

retry_wait=3 # seconds to wait for notebook to start on each retry
retries=9    # number of retries (default: 10 -- 30 seconds)
retry=0
token=""
while [[ $retry -le $retries && $token = "" ]]
do
    sleep $retry_wait
    jupyter_server_list=$(${CONTACT_CMD} $master_node -- "jupyter-notebook list" 2>&1)
    server_info=$(echo "$jupyter_server_list" | grep "127.0.0.1")
    server=(${server_info//::/ })
    token=(${server//token=/ })
    retry=$((retry+1))
done
echo "Token: ${token[1]}"


#########################################################
# USAGE EXAMPLE:                                        #
#########################################################
#                                                       #
# ./submit_jupyter_job.sh 00:01:00 test_job 2 qos False #
#                                                       #
#########################################################
# Returns the job id, the main node where               #
# Jupyter is running, and the session token.            #
#########################################################
