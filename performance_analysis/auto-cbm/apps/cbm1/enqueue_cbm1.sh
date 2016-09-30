#!/bin/bash

# Get parameters
lang=java	 # java / python
wwd=scratch	 # gpfs / scratch
net=infiniband   # infiniband / ethernet4
log_lvl=debug # off / debug
trace=false # true / false
jobID=None

DIR=$(pwd -P)

# EXPERIMENT PARAMETERS #########
EXPERIMENT_REPETITIONS=1

numTasks=(1024 2048 8192 16384)
numWorkers=(2 4 8 16 32 64)
deepness=(1)
tasksPerNode=(16)
taskSleepTimes=(100 500 2000 5000 10000)

fileClasspaths=("$DIR/target/cbm1.jar")
packagesToRun=(cbm1.Cbm1)
##################################

# SECOND EXPERIMENT PARAMETERS ###
EXPERIMENT_REPETITIONS2=0

numTasks2=(256 512 2048)
numWorkers2=(4)
deepness2=(1)
tasksPerNode2=(16 24 32)
taskSleepTimes2=(100 500 2000)
##################################

##################################

getJobId () # job_dependency
{
	sleep 7
	jobID=$(bjobs | tail -n 1 | cut -c -7)
}

#$1=$workers, $2=numTasks, $3=deepness, $4=sleepTime, $5=tasksPerNode
execute ()
{
	execTime=$(( 5 + ( ($2/($1*16))*$3 * ($4/1000) )/55 ))
	
	for ((i=0; i<${#fileClasspaths[@]}; i=i+1))
	do
		fileClasspath=${fileClasspaths[$i]}
		packageToRun=${packagesToRun[$i]}
		
		getJobId
                enqueue_compss \
			--sc_cfg=default \
			--exec_time=$execTime \
			--num_nodes=$(( $1+1 )) \
			--job_dependency=$jobID \
			--tasks_per_node=$5 \
			--master_working_dir=. \
			--worker_working_dir=$wwd \
			--classpath="$fileClasspath" \
			--network=$net \
			--log_level=$log_lvl \
			--tracing=$trace \
			--graph=$trace \
			$packageToRun $2 $3 $4 $5
	done
}


for((r=1; r<=$EXPERIMENT_REPETITIONS; r=r+1))
do
    for ntasks in "${numTasks[@]}" 
    do
            for w in "${numWorkers[@]}"
            do
                for d in "${deepness[@]}"
                do
		    for tpn in "${tasksPerNode[@]}"
                    do

		       	for t in "${taskSleepTimes[@]}"
		       	do
		 		execute $w $ntasks $d $t $tpn
		       	done
		    done
                done
        done
    done
done


for((r=1; r<=$EXPERIMENT_REPETITIONS2; r=r+1))
do
    for ntasks in "${numTasks2[@]}" 
    do
            for w in "${numWorkers2[@]}"
            do
                for d in "${deepness2[@]}"
                do
		    for tpn in "${tasksPerNode2[@]}"
		    do
			    for t in "${taskSleepTimes2[@]}"
			    do
				execute $w $ntasks $d $t $tpn
			    done
		    done
                done
        done
    done
done

