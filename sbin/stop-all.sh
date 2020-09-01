#!/bin/bash

# check arguments
if [ $# != 0 ]; then
    echo "usage: $(basename $0)"
    exit
fi

# compute project directory and hostfile locations
projectdir="$(pwd)/$(dirname $0)/.."
hostfile="$projectdir/etc/hosts.txt"

# iterate over hosts
nodeid=0
while read line; do
    # parse host
    host=$(echo $line | awk '{print $1}')

    # initialize pidfile
    pidfile="$projectdir/log/node-$nodeid.pid"

    echo "stopping node $nodeid"
    if [ $host == "127.0.0.1" ]; then
        # stop node locally
        kill `cat $pidfile`
        rm $pidfile
    else
        # stop node on remote host
        ssh rammerd@$host -n "kill \$(cat $pidfile); rm $pidfile"
    fi

    # increment node id
    (( nodeid += 1 ))
done <$hostfile
