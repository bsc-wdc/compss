#!/bin/bash

#-------------------------------------
# usage:
# compressWorkingDir.sh [WORKING_DIR] [COMPRESSION_FILE_DST] [DIR_TO_EXCLUDE_1] [...] [DIR_TO_EXCLUDE_N]
# -------------------------------------

    #Get Parameters
    workingDir=$1
    compressionDst=$2
    shift 2
    # shellcheck disable=SC2124
    excludeDir=""
    for folder in "$@"
    do
        excludeDir="${excludeDir} --exclude=${folder}"
    done

    #-------------------------------------
    # Create sandbox
    #-------------------------------------
    if [ ! -d "$workingDir" ]; then
        echo "Working Directory ${workingDir} is not a directory"
        exit 7
    fi

    if ! command -v "tar" &> /dev/null; then
        echo "Command tar could not be found"
        exit 100
    fi

    cd "$workingDir"
    command="tar -zc${excludeDir} -f ${compressionDst} ./*"
    eval "$command"


