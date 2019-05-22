#!/bin/bash -e

module use /lustre/home/nx01/shared/cherold/sw/modules
# module load scorep_python/io scorep_preload/git       # Asgard
module load scorep/icc-openmpi scorep-python/python$1   # Prototype

export SCOREP_PROFILING_ENABLE_CORE_FILES=1
#export SCOREP_FILTERING_FILE=/PATH/TO/pycompss.flt     # lustre/home/cherold/pycompss.flt  # Asgard
export SCOREP_ENABLE_PROFILING=false
export SCOREP_ENABLE_TRACING=true
export SCOREP_TOTAL_MEMORY=500MB
# export SCOREP_SUBSTRATE_PLUGINS=vampir_groups_writer  # Asgard