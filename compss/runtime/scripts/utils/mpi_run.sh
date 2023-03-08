#!/bin/bash

echo "Running mpi_run.sh"
echo "$ENV_LOAD_MODULES_SCRIPT"


ssh -o StrictHostKeyChecking=no $(hostname) /bin/bash <<EOF
cd $COMPSS_MASTER_WORKING_DIR
export COMPSS_MPIRUN_TYPE=$COMPSS_MPIRUN_TYPE

source $ENV_LOAD_MODULES_SCRIPT
echo "$PWD"
ls

$@

EOF
