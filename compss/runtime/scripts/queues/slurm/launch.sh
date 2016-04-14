#!/bin/sh

export IT_HOME=$1
export GAT_LOCATION=$2

SLURM_NODES=$3
TPN=$4
TRACING=$5
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
WORKER_WORKING_DIR=/tmp/

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

ASSIGNED_LIST=`scontrol show hostname $SLURM_NODES`
#ASSIGNED_LIST=$(scontrol show hostname $SLURM_NODES)
echo "Nodes assigned"
echo "$ASSIGNED_LIST"

MASTER_NODE=`hostname`;
echo "Master will run in $MASTER_NODE"

#WORKER_LIST=`echo $ASSIGNED_LIST | sed -e "s/$MASTER_NODE//g"`;
WORKER_LIST=`echo "$ASSIGNED_LIST" | tail -n +2`
echo "List of workers:"
echo "$WORKER_LIST"

AUX_LIST=`echo $WORKER_LIST`;

# Find the number of tasks to be executed on each node
for node in $WORKER_LIST
do
	ntasks=$TPN
	#echo "Num tasks per node is $TPN"
        if [ $ntasks -ne 0 ]
        then
		/bin/cat >> $RESOURCES_FILE << EOT
  <Resource Name="${node}">
    <Capabilities>
      <Host>
        <TaskCount>0</TaskCount>
      </Host>
      <Processor>
        <Architecture>Intel</Architecture>
        <Speed>3.47</Speed>
        <CPUCount>12</CPUCount>
      </Processor>
      <OS>
        <OSType>Linux</OSType>
      </OS>
      <StorageElement>
        <Size>36</Size>
      </StorageElement>
      <Memory>
        <PhysicalSize>24</PhysicalSize>
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

	/bin/cat >> $PROJECT_FILE << EOT
  <Worker Name="${node}">
    <InstallDir>$WORKER_INSTALL_DIR</InstallDir>
    <WorkingDir>$WORKER_WORKING_DIR</WorkingDir>
    <LibraryPath>$LIBRARY_PATH</LibraryPath>
    <LimitOfTasks>$ntasks</LimitOfTasks>
  </Worker>

EOT

        fi
        AUX_LIST=`echo $AUX_LIST | sed -e "s/$node//g"`
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

JAVA_HOME=/etc/alternatives/java_sdk_1.7.0

if [ "$DEBUG" == "true" ]
then
        log_file=$IT_HOME/log/it-log4j.debug
else
        log_file=$IT_HOME/log/it-log4j.info
fi

if [ "$TRACING" == "true" ]
then
	export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$IT_HOME/../extrae/lib
	export EXTRAE_ON=1
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
        -Dit.tracing=$TRACING \
        -Dit.worker.cp=$CP \
	-Dit.script.dir=$IT_HOME/scripts/system \
	-Dit.log.root=${PWD}/${SLURM_JOB_ID}"

        time $JAVACMD integratedtoolkit.loader.ITAppLoader total $APP_NAME $*

	echo "Application finished"

elif [ $LANG = c ]
then
        echo "C language not implemented"

elif [ $LANG = python ]
then
	PYCOMPSS_HOME=$IT_HOME/bindings/python

	export PYTHONPATH=$PYCOMPSS_HOME:$CP
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
-Dit.tracing=$TRACING
-Dit.core.count=$TASK_COUNT
-Dit.worker.cp=$CP
-Dit.script.dir=$IT_HOME/scripts/system
-Dit.log.root=${PWD}/${SLURM_JOB_ID}
EOT

	time python $PYCOMPSS_HOME/pycompss/runtime/launch.py $APP_NAME $*
fi


/bin/rm -rf $PROJECT_FILE
/bin/rm -rf $RESOURCES_FILE


