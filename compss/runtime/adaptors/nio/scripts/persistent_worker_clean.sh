#!/bin/bash

#
# Description:
# Kills all the childs of a given parent pid, included the parent itself
# ps --ppid $PPID -o pid= | awk '{ print $1 }' | xargs kill -15 && kill -15 $1
#

#
# METHODS
#

kill_recursive() {
  if [ $1 -gt 0 ]; then 
     # Active processes
     to_kill=$(ps --ppid "$1" -o pid= | awk '{ print $1 }' | tr '\n' ' ')
 
     # Kill each child process of the parent
     for fn in $to_kill; do
        kill_recursive "$fn"
     done

     # Kill the parent process
     if [ -z "$to_kill" ]; then
        echo "Killing $1"
        kill -15 "$1"
     fi
  fi
}


#
# ENTRY POINT
#

kill_recursive "$1"
