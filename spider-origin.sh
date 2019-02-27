#!/bin/bash
BASEDIR=$(dirname $0)
#echo "Script location: ${BASEDIR}"

python3 $BASEDIR/spider-origin.py "$@"
java -cp ./akka-distributed-workers-assembly-1.0.jar worker.Main spider-origin.sh content $arg '-workerTimeout  1000000'