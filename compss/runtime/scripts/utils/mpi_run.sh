#!/bin/bash

echo "Running mpi_run.sh"
echo "$ENV_LOAD_MODULES_SCRIPT"

env | grep "SLURM_" | sed 's/SLURM/export SLURM/g' | sed 's/=/="/g' | sed -e 's/$/"/' > $COMPSS_MASTER_WORKING_DIR/slurm_vars

env


ssh -o StrictHostKeyChecking=no $(hostname) /bin/bash <<EOF

source $COMPSS_MASTER_WORKING_DIR/slurm_vars

cd $COMPSS_MASTER_WORKING_DIR
export COMPSS_MPIRUN_TYPE=$COMPSS_MPIRUN_TYPE

source $ENV_LOAD_MODULES_SCRIPT
echo "$PWD"
ls

$@

EOF
