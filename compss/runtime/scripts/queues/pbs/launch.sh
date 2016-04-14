#!/bin/sh

export IT_HOME=$1
export GAT_LOCATION=$2

PBS_NODEFILE=$3
TPN=$4
UR=$5
MONITORING=$6
DEBUG=$7
CP=$8
GRAPH=$9
LANG=${10}
TASK_COUNT=${11}
LIBRARY_PATH=${12}
APP_NAME=${13}

shift 13

WORKER_INSTALL_DIR=$IT_HOME/scripts/system/
WORKER_WORKING_DIR=/scratch.local/tmp_compss_${PBS_JOBID}/
echo "Working dir will be $WORKER_WORKING_DIR"
if [ ! -d "$WORKER_WORKING_DIR" ]
then
	mkdir $WORKER_WORKING_DIR
	if [ $? -ne 0 ]
        then
		echo "Cannot create working directory $WORKER_WORKING_DIR at node `hostname`"
		exit 1
	fi
fi


sec=`/bin/date +%s`
RESOURCES_FILE=$WORKER_WORKING_DIR/resources_$sec.xml
PROJECT_FILE=$WORKER_WORKING_DIR/project_$sec.xml



# Begin creating the resources file and the project file

/bin/cat > $RESOURCES_FILE << EOT
<?xml version="1.0" encoding="UTF-8"?>
<ResourceList>

  <Disk Name="gpfs">
    <MountPoint>/gpfs</MountPoint>
  </Disk>

EOT

/bin/cat > $PROJECT_FILE << EOT
<?xml version="1.0" encoding="UTF-8"?>
<Project>


EOT

 
#ASSIGNED_LIST=`cat $PBS_NODEFILE | sed -e 's/\.[^\ ]*//g'`
#echo "Node list assigned is:"
#echo "$ASSIGNED_LIST"

MASTER_NODE=`hostname`;
echo "Master will run in $MASTER_NODE"

# Get node list
#WORKER_LIST=`echo $ASSIGNED_LIST | sed -e "s/$MASTER_NODE//g"`;
# To remove only once: WORKER_LIST=\`echo \$ASSIGNED_LIST | sed -e "s/\$MASTER_NODE//"\`;
WORKER_LIST=`uniq $PBS_NODEFILE`
echo "List of workers:"
echo "$WORKER_LIST"

# Find the number of tasks to be executed on each node
for node in $WORKER_LIST
do
        #ntasks=`echo $AUX_LIST | sed -e 's/\ /\n/g' | grep $node | wc -l`
	ntasks=$TPN
        if [ $ntasks -ne 0 ]
        then
		if [ "${node}" = "${MASTER_NODE}" ]
                then
                        # Keep 6 cores for the master runtime
                        let ntasks=$ntasks-6
		else
			ssh ${node} "if [ ! -d \"$WORKER_WORKING_DIR\" ]; then mkdir $WORKER_WORKING_DIR; fi";
                	#ssh $node "mkdir $WORKER_WORKING_DIR"
			if [ $? -ne 0 ]
                	then
                        	echo "Cannot create working directory $WORKER_WORKING_DIR at node $node"
                        	exit 1
                	fi
                fi
		/bin/cat >> $RESOURCES_FILE << EOT
  <Resource Name="${node}">
    <Capabilities>
      <Host>
        <TaskCount>0</TaskCount>
      </Host>
      <Processor>
        <Architecture>Intel</Architecture>
        <Speed>2.3</Speed>
        <CPUCount>16</CPUCount>
      </Processor>
      <OS>
        <OSType>Linux</OSType>
      </OS>
      <StorageElement>
        <Size>36</Size>
      </StorageElement>
      <Memory>
        <PhysicalSize>32</PhysicalSize>
      </Memory>
      <ApplicationSoftware>
        <Software>COMPSs</Software>
        <Software>JavaGAT</Software>
      </ApplicationSoftware>
      <FileSystem/>
      <NetworkAdaptor/>
    </Capabilities>
    <Requirements/>
    <Disks>
      <Disk Name="gpfs">
	<MountPoint>/gpfs</MountPoint>
      </Disk>
    </Disks>
  </Resource>

EOT
		echo "Node ${node} is assigned $ntasks slots" 
		/bin/cat >> $PROJECT_FILE << EOT
  <Worker Name="${node}">
    <InstallDir>$WORKER_INSTALL_DIR</InstallDir>
    <WorkingDir>$WORKER_WORKING_DIR</WorkingDir>
    <LimitOfTasks>$ntasks</LimitOfTasks>
  </Worker>

EOT

        fi
done


# Finish the resources file and the project file 

/bin/cat >> $RESOURCES_FILE << EOT
</ResourceList>
EOT

/bin/cat >> $PROJECT_FILE << EOT
</Project>
EOT

echo "Generation of resources and project file finished"


# Launch the application with COMPSs

JAVA_HOME=/usr/lib/jvm/java-1.6.0-openjdk.x86_64

if [ "$DEBUG" == "true" ]
then
        log_file=$IT_HOME/log/it-log4j.debug
else
        log_file=$IT_HOME/log/it-log4j.info
fi

if [ $LANG = java ]
then
        JAVACMD=$JAVA_HOME/bin/java" \
        -classpath $CP:$IT_HOME/rt/compss-rt.jar \
        -Dlog4j.configuration=$log_file \
	-Dgat.adaptor.path=$GAT_LOCATION/lib/adaptors \
        -Dit.to.file=false \
        -Dit.gat.broker.adaptor=sshtrilead \
        -Dit.gat.file.adaptor=sshtrilead \
        -Dit.lang=$LANG \
        -Dit.project.file=$PROJECT_FILE \
        -Dit.resources.file=$RESOURCES_FILE \
	-Dit.project.schema=$IT_HOME/xml/projects/project_schema.xsd \
	-Dit.resources.schema=$IT_HOME/xml/resources/resource_schema.xsd \
        -Dit.appName=$APP_NAME \
        -Dit.graph=$GRAPH \
        -Dit.monitor=$MONITORING \
        -Dit.tracing=$UR \
        -Dit.worker.cp=$CP
	-Dit.log.root=${PWD}/${PBS_JOBID} "

        $JAVACMD integratedtoolkit.loader.ITAppLoader total $APP_NAME $*

	echo "Application finished"

elif [ $LANG = c ]
then
        echo "C language not implemented"

elif [ $LANG = python ]
then
	PYCOMPSS_HOME=$IT_HOME/bindings/python

	export PYTHONPATH=$PYCOMPSS_HOME/pycompss:$CP
        export LD_LIBRARY_PATH=$LIBRARY_PATH:$JAVA_HOME/jre/lib/amd64/server:$IT_HOME/bindings/c/lib:$LD_LIBRARY_PATH

        jvm_options_file=`mktemp`
        if [ $? -ne 0 ]
        then
                echo "Can't create temp file for JVM options, exiting..."
                exit 1
        fi
        export JVM_OPTIONS_FILE=$jvm_options_file

        app_no_py=$(basename "$APP_NAME" ".py")
        /bin/cat >> $jvm_options_file << EOT
-Djava.class.path=$IT_HOME/rt/compss-rt.jar
-Dlog4j.configuration=$log_file
-Dgat.adaptor.path=$GAT_LOCATION/lib/adaptors
-Dit.to.file=false
-Dit.gat.broker.adaptor=sshtrilead
-Dit.gat.file.adaptor=sshtrilead
-Dit.lang=$LANG
-Dit.project.file=$PROJECT_FILE
-Dit.resources.file=$RESOURCES_FILE
-Dit.project.schema=$IT_HOME/xml/projects/project_schema.xsd
-Dit.resources.schema=$IT_HOME/xml/resources/resource_schema.xsd
-Dit.appName=$app_no_py
-Dit.graph=$GRAPH
-Dit.monitor=$MONITORING
-Dit.tracing=$UR
-Dit.core.count=$TASK_COUNT
-Dit.worker.cp=$CP
-Dit.log.root=${PWD}/${PBS_JOBID}
EOT

        time python $PYCOMPSS_HOME/pycompss/runtime/launch.py $APP_NAME $*
	
	rm $jvm_options_file
fi

for node in $WORKER_LIST
do
	ssh ${node} "rm -rf $WORKER_WORKING_DIR"
done	

/bin/rm -rf $PROJECT_FILE
/bin/rm -rf $RESOURCES_FILE

