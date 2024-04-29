#!/bin/bash

echo "Running NEW mpi_run.sh"
echo "$ENV_LOAD_MODULES_SCRIPT"
WORKING_DIR=$PWD

echo "mpi_run.sh Working Dir: $WORKING_DIR"

env | grep "SLURM_" | sed 's/SLURM/export SLURM/g' | sed 's/=/="/g' | sed -e 's/$/"/' > ${WORKING_DIR}/slurm_vars

env | grep "SLURM_" | sed 's/SLURM/export SINGULARITYENV_SLURM/g' | sed 's/=/="/g' | sed -e 's/$/"/' >> $WORKING_DIR/slurm_vars


ssh -o StrictHostKeyChecking=no $(hostname) /bin/bash <<EOF

source ${STORAGE_ENVS}

source ${WORKING_DIR}/slurm_vars

cd $WORKING_DIR
export COMPSS_MPIRUN_TYPE=$COMPSS_MPIRUN_TYPE

source $ENV_LOAD_MODULES_SCRIPT

$@

EOF
