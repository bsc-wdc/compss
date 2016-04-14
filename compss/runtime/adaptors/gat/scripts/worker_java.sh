#!/bin/bash

app_dir=$1
cp=$2
debug=$3
app=$4
method=$5
has_target=$6
nparams=$7
shift 7
params=$*

if [ "$debug" == "true" ]
then
    echo app_dir $app_dir
    echo classpath $cp
    echo debug $debug
    echo app $app
    echo method $method
    echo has_target $has_target
    echo nparams $nparams
    echo params $params
fi
    
add_to_classpath () {
        DIRLIBS=${1}/*.jar
        for i in ${DIRLIBS}
        do
                if [ "$i" != "${DIRLIBS}" ] ; then
                        CLASSPATH=$CLASSPATH:"$i"
                fi
        done
}

add_to_classpath "$app_dir"
add_to_classpath "$app_dir/lib"
script_dir=`dirname $0`
compss_jar=$script_dir/../../../../adaptors/gat/worker/compss-adaptors-gat-worker.jar

# Launch the JVM to run the task
java -Xms128m -Xmx2048m -classpath $cp:$CLASSPATH:$app_dir:$compss_jar integratedtoolkit.gat.worker.GATWorker $debug $app $method $has_target $nparams $params

exit $?

