#!/bin/bash
# Author: Sergio Rodriguez Guasch < sergio.rodriguez at bsc dot es >
# This script builds a bundle with all the needed stuff (aside from Redis backend)
# to use the Redis storage API with COMPSs

# Rebuild the Redis API. This should leave a directory target with an uber-JAR that contains
# all the needed stuff
mvn clean package

BUNDLE_NAME=COMPSs-Redis-bundle
# Delete (if any) old Bundle directory
rm -rf $BUNDLE_NAME
# Create the bundle directory. This will be the path that we will need to pass to enqueue_compss as
# storage_home
mkdir -p $BUNDLE_NAME
# Copy the Java JAR that contains the implementation of the Redis API
cp target/compss-redisPSCO.jar $BUNDLE_NAME
# Remove .pyc files from Python code
rm -f python/storage/*.pyc
# Copy Python API
cp -rf python $BUNDLE_NAME
# Move the scripts folder to the bundle. The scripts "storage_init" and "storage_stop" will be executed
# by COMPSs at the beggining and the end of the execution respectively. They build (destroy) a Redis cluster
# They come with some examples
cp -rf scripts $BUNDLE_NAME
