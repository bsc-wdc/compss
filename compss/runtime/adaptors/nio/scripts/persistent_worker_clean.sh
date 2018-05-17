#!/bin/bash

#Kills all the childs of a given parent pid, included the parent itself
#ps --ppid $PPID -o pid= | awk '{ print $1 }' | xargs kill -15 && kill -15 $1

kill_recursive() {
        to_kill=$(ps --ppid $1 -o pid= | awk '{ print $1 }' | tr '\n' ' ')

        for fn in $to_kill # For each child pid of the parent
        do
                kill_recursive $fn # Kill the first one
        done

        if [ -z $to_kill ]
        then
                kill -15 $1
                #echo "killing $1"
        fi
}

kill_recursive "$1"
