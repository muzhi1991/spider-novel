#!/bin/bash
BASEDIR=$(dirname $0)
#echo "Script location: ${BASEDIR}"

python3 $BASEDIR/spider-origin.py "$@"
