#!/bin/bash

job_name=$1      # Not supported yet
exec_time=$2     # Walltime in minutes
num_nodes=$3     # Number of nodes
qos=$4           # Quality of service
tracing=$5       # Tracing
classpath=$6     # Classpath
pythonpath=$7    # Pythonpath
storage_home=$8  # Storage home path
storage_props=$9 # Storage properties file
storage=${10}    # Storage shortcut to use


###############################################
#     Get the Supercomputer configuration     #
###############################################
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# Load default CFG for default values
DEFAULT_SC_CFG="default"
defaultSC_cfg=${SCRIPT_DIR}/../../queues/cfgs/${DEFAULT_SC_CFG}.cfg
#shellcheck source=${SCRIPT_DIR}/../../queues/cfgs/default.cfg
source "${defaultSC_cfg}"
defaultQS_cfg=${SCRIPT_DIR}/../../queues/${QUEUE_SYSTEM}/${QUEUE_SYSTEM}.cfg
#shellcheck source=${SCRIPT_DIR}/../../queues/slurm/slurm.cfg
source "${defaultQS_cfg}"

###############################################
#               Shortcut checks               #
###############################################
if [ "${storage}" = "None" ] || [ "${storage}" = "" ]; then
    # Continue as normal
    true
elif [ "${storage}" = "Redis" ]; then
    # Override variables to use Redis
    classpath="${SCRIPT_DIR}/../../../../Tools/storage/redis/compss-redisPSCO.jar:${classpath}"
    pythonpath="${SCRIPT_DIR}/../../../../Tools/storage/redis/python:${pythonpath}"
    storage_home="${SCRIPT_DIR}/../../../../Tools/storage/redis"
    if [ ! -f ${HOME}/storage_props.txt ]; then
        # If not exist, then create an empty one.
        # Otherwise, use the storage_props.cfg file
        touch ${HOME}/storage_props.cfg
    fi
    storage_props="${HOME}/storage_props.cfg"
else
    # Unsupported storage
    echo "ERROR: Non supported storage shortcut."
    exit 1
fi


###############################################
#       Submit the jupyter notebook job       #
###############################################
if [ ${storage_home} = "undefined" ]; then
    result=$(enqueue_compss --exec_time=${exec_time} --num_nodes=${num_nodes} --qos=${qos} --tracing=${tracing} --classpath=${classpath} --pythonpath=${pythonpath}:${HOME} --lang=python --jupyter_notebook)
else
    export CLASSPATH=${classpath}:${CLASSPATH}
    export PYTHONPATH=${pythonpath}:${PYTHONPATH}
    result=$(enqueue_compss --exec_time=${exec_time} --num_nodes=${num_nodes} --qos=${qos} --tracing=${tracing} --classpath=${classpath} --pythonpath=${pythonpath}:${HOME} --storage_home=${storage_home} --storage_props=${storage_props} --lang=python --jupyter_notebook)
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


################################################################################################
# USAGE EXAMPLE:                                                                               #
################################################################################################
#                                                                                              #
# ./submit_jupyter_job.sh 00:01:00 test_job 2 qos False $(pwd) $(pwd) undefined undefined None #
#                                                                                              #
################################################################################################
# Returns the job id, the main node where Jupyter is running, and the session token.           #
################################################################################################
