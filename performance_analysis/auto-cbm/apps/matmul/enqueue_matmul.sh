#!/bin/bash 

# Get parameters
lang=java	 # java / python
net=infiniband   # infiniband / ethernet4
log_lvl=off      # off / debug
trace=false      # true / false
jobID=None

# EXPERIMENT PARAMETERS #########
EXPERIMENT_REPETITIONS=1

workingDirs=(scratch gpfs)
numWorkers=(4)
BSIZES=(8 16 32)
MSIZES=(64 128 256) # The min of this must be greater than the max of BSIZES
packagesToRun=(matmul.objects.Matmul)
##################################


getJobId () # job_dependency
{
	sleep 7
	jobID=$(bjobs | tail -n 1 | cut -c -7)
}

#$1=$workingDir, $2=numWorkers, $3=BSIZE, $4=MSIZE
execute ()
{
	workingDir=$1
	nWorkers=$2
	BSIZE=$3
	MSIZE=$4
	execTime=$(( 5 + $BSIZE/8 ))
 	
	for ((i=0; i<${#packagesToRun[@]}; i=i+1))
	do
		packageToRun=${packagesToRun[$i]}
		
		for mode in "${modes[@]}"
		do
		
			    getJobId
			    enqueue_compss \
					--exec_time=$execTime \
					--num_nodes=$(( $nWorkers+1 )) \
					--queue_system=lsf \
					--job_dependency=$jobID \
					--tasks_per_node=16 \
					--master_working_dir=. \
					--worker_working_dir=$workingDir \
					--classpath="$fileClasspath" \
					--network=$net \
					--log_level=$log_lvl \
					--tracing=$trace \
					--graph=$trace \
					$packageToRun $BSIZE $MSIZE
		done
	done
}

for((r=1; r<=$EXPERIMENT_REPETITIONS; r=r+1))
do
    for wwd in "${workingDirs[@]}" 
    do
	for bsize in "${BSIZES[@]}" 
	do
	  for msize in "${MSIZES[@]}" 
	  do
	      for w in "${numWorkers[@]}"
	      do
		  execute $wwd $w $bsize $msize
	      done
	  done
       done
    done
done                       
