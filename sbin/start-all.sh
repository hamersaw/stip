#!/bin/bash

# check arguments
if [ $# != 0 ]
then
    echo "usage: $(basename $0)"
    exit
fi

# compute project directory and hostfile locations
projectdir="$(pwd)/$(dirname $0)/.."
hostfile="$projectdir/etc/hosts.txt"

# initialize instance variables
if [ -f "$projectdir/impl/stipd/target/debug/stipd" ]; then
    application="$projectdir/impl/stipd/target/debug/stipd"
fi

if [ -f "$projectdir/impl/target/debug/stipd" ]; then
    application="$projectdir/impl/target/debug/stipd"
fi

if [ -z "$application" ]; then
    echo "'stipd' binary not found."
    exit
fi

# iterate over hosts
nodeid=0
while read line; do
    # parse host, port, and options
    host=$(echo $line | awk '{print $1}')
    gossipport=$(echo $line | awk '{print $2}')
    rpcport=$(echo $line | awk '{print $3}')
    xferport=$(echo $line | awk '{print $4}')
    options=$(echo $line | cut -d' ' -f5-)

    # handle seed address
    if [ ! -z "$seedaddr" ]; then
        options="$options -s $seedaddr -e $seedport"
    fi

    seedaddr=$host
    seedport=$port

    echo "starting node $nodeid"
    if [ $host == "127.0.0.1" ]; then
        # start application locally
        RUST_LOG=debug,h2=info,hyper=info,tower_buffer=info \
            $application $nodeid -i $host -p $gossipport \
            -r $rpcport -x $xferport $options \
                > $projectdir/log/node-$nodeid.log 2>&1 &

        echo $! > $projectdir/log/node-$nodeid.pid
    else
        # start application on remote host
        ssh rammerd@$host -n "RUST_LOG=info \
            $application $nodeid -i $host -p $gossipport \
            -r $rpcport -x $xferport $options \
                > $projectdir/log/node-$nodeid.log 2>&1 & \
            echo \$! > $projectdir/log/node-$nodeid.pid"
    fi

    # increment node id
    (( nodeid += 1 ))
done <$hostfile
