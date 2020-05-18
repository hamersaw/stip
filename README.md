# stip (SpatioTemporal Image Partitioner)
## OVERVIEW
A distributed spatiotemporal image management framework designed specifically for training neural networks.

## WORKSPACE
The implementation is structured using rust's workspace paradigm within the ./impl directory in the project root.
#### PROTOBUF
This project uses [gRPC](https://grpc.io/) and [Protocol Buffers](https://developers.google.com/protocol-buffers/) to present a language agnostic RPC interface. This paradigm is employed for all system communication (except data transfers). The protobuf rust crate includes protobuf compilation instructions along with project module export definitions.
#### STIP
This is the command line application for interfacing with the stip cluster. It includes a variety of testing and operational functionality explored further in the 'COMMANDS' section below.
#### STIPD
This crate defines a stip node. It contains the bulk of the implementation; defining image partioning and distribution strategies and metadata queres among other functionality.

## COMMANDS
### STIPD
#### START CLUSTER
The cluster deployment is provided in the ./etc/hosts.txt file, where each row defines a single cluster node. Each row is formatted as 'IpAddress GossipPort RpcPort XferPort FLAGS...'. An example row is:
 
    127.0.0.1 15605 15606 15607 -d /tmp/STIP/0 -t 0 -t 6148914691236516864 -t 12297829382473033728

This row defines a stipd node running at the provided IP address (127.0.0.1) and ports (15605 15606 15607). Additionally it defines a variety of command line arguments including: -d <directory> to define the storage directory and -t <token> to initialize this node with the provided DHT tokens. 

Starting the cluster leverages the provided ./sbin/start-all.sh script. This script simply iterates over nodes defined in ./etc/hosts.txt and starts a node instance on the provided machine. It should be noted that starting nodes on remote hosts requires ssh access.

    # terminal command to start stip cluster from root project
    ./sbin/start-all.sh
#### STOP CLUSTER
Similar to starting the cluster, the ./sbin/stop-all.sh script has been provided to stop a stip cluster. Again, this script leverages the ./etc/hosts.txt file to iterate over node definitions.

    # terminal command to stop stip cluster from root project
    ./sbin/stop-all.sh
### STIP
#### CLUSTER LIST / SHOW
TODO
#### TASK LIST / SHOW
TODO
#### DATA LOAD
TODO
#### DATA FILL / SPLIT
TODO
#### DATA LIST / SEARCH
TODO

## TODO
- add Filter protobuf -> use everywhere
- image replication? - one replica on geohash of length (x - 1)
- improve node logging
- __multithread image loading - cpu usage is very low__
- refactor task implementations - facilitate code reuse
#### COMMANDS 
- __data load - support MODIS data__
- data merge - combine images into higher level images
- **cloud coverage - computation on images**
- task stop?
