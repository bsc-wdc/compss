#!/bin/sh

##############################################################
# SCRIPT FOR SUBMISSION OF APPLICATIONS TO SLURM WITH COMPSs #
##############################################################


# Function that converts a cost in minutes to an expression of wall clock limit for slurm
convert_to_wc()
{
        cost=$1
        wc_limit=":00"

        min=`expr $cost % 60`
        if [ $min -lt 10 ]
        then
                wc_limit=":0${min}${wc_limit}"
        else
                wc_limit=":${min}${wc_limit}"
        fi

        hrs=`expr $cost / 60`
        if [ $hrs -gt 0 ]
        then
                if [ $hrs -lt 10 ]
                then
                        wc_limit="0${hrs}${wc_limit}"
                else
                        wc_limit="${hrs}${wc_limit}"
                fi
        else
                wc_limit="00${wc_limit}"
        fi
}


# Parameters: num_nodes tasks_per_node wc_minutes loader ur_creation classpath app_name app_params
NNODES=$1
if [ $NNODES -lt 2 ]
then
echo "Error: at least 2 nodes needed, exiting"
exit 1
fi
TPN=$2
convert_to_wc $3
TRACING=$4
MONITORING=$5
DEBUG=$6
CP=$7
GRAPH=$8
WORKING_DIR=$9
LANG=${10}
TASK_COUNT=${11}
LIBRARY_PATH=${12}
shift 12 
EOT="EOT"


TMP_SUBMIT_SCRIPT=`mktemp`
echo "Temp submit script is: $TMP_SUBMIT_SCRIPT"
if [ $? -ne 0 ]
then
	echo "Can't create temp file, exiting..."
	exit 1
fi

#TMPPREFIX=COMPSs
#TMPDIR=`mktemp -d`
#if [ $? -ne 0 ]; then
#        echo "Can't create temp dir, exiting..."
#        exit 1
#fi

script_dir=`dirname $0`
IT_HOME=/opt/COMPSs/Runtime
GAT_LOCATION=$IT_HOME/../JAVA_GAT

/bin/cat >> $TMP_SUBMIT_SCRIPT << EOT
#!/bin/bash
#
#SBATCH --job-name=COMPSs
#SBATCH --nodes=$NNODES
#SBATCH --partition=interactive
#SBATCH --time=$wc_limit
#SBATCH --output=compss_${NNODES}_%j.out
#SBATCH --error=compss_${NNODES}_%j.err

$script_dir/launch.sh $IT_HOME $GAT_LOCATION \$SLURM_JOB_NODELIST $TPN $TRACING $MONITORING $DEBUG $CP $GRAPH $LANG $TASK_COUNT $LIBRARY_PATH $*

EOT


# Check if the creation of the script failed
result=$?
if [ $result -ne 0 ]
then
	echo "Error creating the submit script" >&2
        exit -1
fi

# Submit the job to the queue
sbatch $TMP_SUBMIT_SCRIPT 1>$TMP_SUBMIT_SCRIPT.out 2>$TMP_SUBMIT_SCRIPT.err
result=$?

# Cleanup
submit_err=`/bin/cat $TMP_SUBMIT_SCRIPT.err`
/bin/rm -rf $TMP_SUBMIT_SCRIPT.*

# Check if submission failed
if [ $result -ne 0 ]
then
	echo "Error submitting the job" >&2
	echo $submit_err >&2
        exit -1 
fi

