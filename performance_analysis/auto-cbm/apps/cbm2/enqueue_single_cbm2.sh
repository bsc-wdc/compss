#!/bin/bash 

# Get parameters
lang=java	 # java / python
net=infiniband   # infiniband / ethernet4
log_lvl=off      # off / debug
trace=false      # true / false
elasticity=2
jobID=None

FILES_CLASSPATH="$(pwd -P)/target/cbm2Files.jar"
OBJECTS_CLASSPATH="$(pwd -P)/target/cbm2Objects.jar"

# EXPERIMENT PARAMETERS #########
EXPERIMENT_REPETITIONS=1

workingDirs=(gpfs)
numTasks=(128)
deepness=(10)
numWorkers=(4)
txSizes=(500000) # BYTES
taskSleepTimes=(1000)
#numWorkers=(4 8)
#txSizes=(500000 10000000 50000000) # BYTES

fileClasspaths=($OBJECTS_CLASSPATH)
packagesToRun=(cbm2.objects.Cbm2)
modes=(INOUT)
##################################


#$1=$workers, $2=numTasks, $3=deepness, $4=sleepTime, $5=txSizes, $6=wwd
execute ()
{
	sizeLog=$(echo "l($5)/l(10)" | bc -l | cut -d"." -f1)
	execTime=$(( 5 + $2/16 + $sizeLog*2 ))
 	
	for ((i=0; i<${#fileClasspaths[@]}; i=i+1))
	do
		fileClasspath=${fileClasspaths[$i]}
		packageToRun=${packagesToRun[$i]}
		
		for mode in "${modes[@]}"
		do
		
			    enqueue_compss \
					--exec_time=$execTime \
					--num_nodes=$(( $1+1 )) \
					--queue_system=lsf \
					--job_dependency=$jobID \
					--cpus_per_node=16 \
					--master_working_dir=. \
					--worker_working_dir=$6 \
					--classpath="$fileClasspath" \
					--network=$net \
					--log_level=$log_lvl \
					--tracing=$trace \
					--graph=$trace \
					--elasticy=$elasticity \
					$packageToRun $2 $3 $4 $5 $mode
		done
	done
}

for((r=1; r<=$EXPERIMENT_REPETITIONS; r=r+1))
do
    for wwd in "${workingDirs[@]}" 
    do
	    for ntasks in "${numTasks[@]}" 
	    do
		for s in "${txSizes[@]}" 
		do
		    for w in "${numWorkers[@]}"
		    do
			for d in "${deepness[@]}"
			do
			    for t in "${taskSleepTimes[@]}"
			    do
				execute $w $ntasks $d $t $s $wwd
			    done
			done
		    done
		done
	    done
    done
done

