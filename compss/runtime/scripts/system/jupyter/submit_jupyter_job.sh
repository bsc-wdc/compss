#!/bin/bash

job_name=$1   # not supported yet
exec_time=$2  # walltime in minutes
num_nodes=$3  # number of nodes
qos=$4        # quality of service
tracing=$5    # tracing

###############################################
#       Submit the jupyter notebook job       #
###############################################

result=$(enqueue_compss --exec_time=${exec_time} --num_nodes=${num_nodes} --qos=${qos} --tracing=${tracing} --lang=python --jupyter_notebook)
submit_line=$(echo "$result" | grep "Submitted")
job_id=(${submit_line//Submitted batch job/ })
echo "JobId: $job_id"

# Get the project and resources xmls path and export them as environment variables to be used from PyCOMPSs binding
#project_line=$(echo "$result" | grep "Project.xml")
#resource_line=$(echo "$result" | grep "Resources.xml")
#project=(${project_line//Project.xml:/ })
#resources=(${project_line//Resources.xml:/ })
#export COMPSS_PROJECT_XML="${project}"
#export COMPSS_RESOURCES_XML="${resources}"


###############################################
#         Wait for the job to start           #
###############################################

function job_is_pending {
    status=$(squeue --job ${job_id} -h -o %T)
    if [ $status = "RUNNING" ]; then
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

nodes=$(squeue --job ${job_id} -h -O nodelist)
expanded_nodes=$(scontrol show hostname ${nodes} | paste -d, -s)
nodes_array=(${expanded_nodes//,/ })
main_node=${nodes_array[0]}
other_nodes=${nodes_array[@]:1}
# echo "Assigned_nodes: $nodes"
# echo "Expanded_assigned_nodes: $expanded_nodes"
echo "MainNode: $main_node"
# echo "OtherNodes: $other_nodes"


###############################################
#      Get the Jupyter-notebook token         #
###############################################

retry_wait=3 # wait for notebook to start
retries=3
retry=0
token=""
while [[ $retry -le $retries && $token = "" ]]
do
    sleep $retry_wait
    jupyter_server_list=$(ssh $main_node -- "jupyter-notebook list" 2>&1)
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
