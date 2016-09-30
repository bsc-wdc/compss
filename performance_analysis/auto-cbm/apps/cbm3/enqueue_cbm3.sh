#!/bin/bash 

# Get parameters
lang=java	 # java / python
net=infiniband   # infiniband / ethernet4
log_lvl=off      # off / debug
trace=false      # true / false
jobID=None


FILES_CLASSPATH="$(pwd -P)/target/cbm3Files.jar"
OBJECTS_CLASSPATH="$(pwd -P)/target/cbm3Objects.jar"

# EXPERIMENT PARAMETERS #########
EXPERIMENT_REPETITIONS=1

workingDirs=(scratch gpfs)
txSizes=(500000 1000000) # BYTES
numWorkers=(4)
deepness=(6)
taskSleepTimes=(10)
#txSizes=(500000 10000000 50000000) # BYTES
#numWorkers=(4 8)


fileClasspaths=($FILES_CLASSPATH $OBJECTS_CLASSPATH)
packagesToRun=(cbm3.files.Cbm3 cbm3.objects.Cbm3)
modes=(IN INOUT)
##################################



getJobId () # job_dependency
{
	sleep 7
	jobID=$(bjobs | tail -n 1 | cut -c -7)
}

#$1=$workers, $2=deepness, $3=sleepTime, $4=txSize, $5=wwd
execute ()
{
	sizeLog=$(echo "l($4)/l(10)" | bc -l | cut -d"." -f1)
	execTime=$(( 5 + $2*3 + $sizeLog*2 ))
	
	for ((i=0; i<${#fileClasspaths[@]}; i=i+1))
	do
		fileClasspath=${fileClasspaths[$i]}
		packageToRun=${packagesToRun[$i]}
		
		for mode in "${modes[@]}"
		do
		
			    getJobId
			    enqueue_compss \
					--exec_time=$execTime \
					--num_nodes=$(( $1+1 )) \
					--queue_system=lsf \
					--job_dependency=$jobID \
					--tasks_per_node=16 \
					--master_working_dir=. \
					--worker_working_dir=$5 \
					--classpath="$fileClasspath" \
					--network=$net \
					--log_level=$log_lvl \
					--tracing=$trace \
					--graph=$trace \
					$packageToRun $2 $3 $4 $mode
		done
	done
}

for((r=1; r<=$EXPERIMENT_REPETITIONS; r=r+1))
do
	for wwd in "${workingDirs[@]}"
	do
	    for s in "${txSizes[@]}" 
	    do
		for w in "${numWorkers[@]}"
		do
		    for d in "${deepness[@]}"
		    do
			for t in "${taskSleepTimes[@]}"
			do
			    execute $w $d $t $s $wwd
			done
		    done
		done
	    done
	done
done

