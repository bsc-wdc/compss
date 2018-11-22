#!/bin/bash

job_id=$1  # Job id


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
#       Cancel the jupyter notebook job       #
###############################################
$(${QUEUE_JOB_CANCEL_CMD})
