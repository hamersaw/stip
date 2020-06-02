# stip (SpatioTemporal Image Partitioner)
## OVERVIEW
A distributed spatiotemporal image management framework designed specifically for training neural networks.

## WORKSPACE
The implementation is structured using rust's workspace paradigm within the ./impl directory in the project root.
#### PROTOBUF
This project uses [gRPC](https://grpc.io/) and [Protocol Buffers](https://developers.google.com/protocol-buffers/) to present a language agnostic RPC interface. This paradigm is employed for all system communication (except data transfers). The protobuf rust crate includes protobuf compilation instructions along with project module export definitions.
#### STIP
This is the command line application for interfacing with the stip cluster. It includes a variety of testing and operational functionality explored further in the [COMMANDS](#COMMANDS) section below.
#### STIPD
This crate defines a stip node. It contains the bulk of the implementation; defining image partioning and distribution strategies and metadata queries among other functionality.

## COMMANDS
### STIPD
#### START CLUSTER
The cluster deployment is provided in the ./etc/hosts.txt file, where each row defines a single cluster node. Each row is formatted as 'ip_address gossip_port rpc_port xfer_portFLAGS...'. An example row is:
 
    127.0.0.1 15605 15606 15607 -d /tmp/STIP/0 -t 0 -t 6148914691236516864 -t 12297829382473033728

This row defines a stipd node running at the provided IP address (127.0.0.1) and ports (15605 15606 15607). Additionally it defines a variety of command line arguments including: -d <directory> to define the image storage directory and -t <token> to initialize this node with the provided DHT tokens. 

Starting the cluster leverages the provided ./sbin/start-all.sh script. This script simply iterates over nodes defined in ./etc/hosts.txt and starts a node instance on the provided machine. It should be noted that starting nodes on remote hosts requires ssh access.

    # terminal command to start stip cluster from root project
    ./sbin/start-all.sh
#### STOP CLUSTER
Similar to starting the cluster, the ./sbin/stop-all.sh script has been provided to stop a stip cluster. Again, this script leverages the ./etc/hosts.txt file to iterate over node definitions.

    # terminal command to stop stip cluster from root project
    ./sbin/stop-all.sh
### STIP
#### CLUSTER LIST / SHOW
These commands are useful for identifying nodes within the cluster. They are typically used for testing or used in the background of APIs or applications when contacting each cluster node is necessary for a particular operation.

    # list all nodes in the cluser
    ./stip cluster list

    # display information about a single cluster node
    ./stip cluster show 0
#### TASK LIST / SHOW
Behind the scenes of stip all functionality is partitioned into a variety of tasks. Said functionality includes data loading, data splitting / merging, data filling, etc. The 'task' interface is used to monitor progress of cluster tasks.
    
    # list all cluster tasks
    ./stip task list

    # display information about a single cluster task
    ./stip task show 1000
#### DATA LOAD
Data load tasks are initialized on a per-node basis, meaning each node ony processes local data. Therefore, data is typically distributed among cluster nodes to enable distributed processing. As such, a separate task must be manually started on each node to load the local data.

    # load a single modis file at geohash length 3
    ./stip data load '~/Downloads/earth-explorer/modis/MCD43A4.A2020100.h08v05.006.2020109032339.hdf' modis -t 1 -l 3

    # load data for the given glob with 4 threads at geohash length 6
    ./stip data load '~/Downloads/earth-explorer/naip/test/*' naip -t 4 -l 6

    # load sentinel data for files with the provided glob at geohash
    #   length 5 using 2 threads and setting the task id as 1000
    ./stip -i $(curl ifconfig.me) data load "/s/$(hostname)/a/nobackup/galileo/usgs-earth-explorer/sentinel-2/foco-20km/*T13TEE*" sentinel -t 2 -l 5 -d 1000
#### DATA LIST / SEARCH
These commands enable searching the system for images using the metadata provided. 'data search' provides an agglomerated data representation, presenting image geohash precision counts satisfying the query. It is useful for gaining understanding of the dataspace. With an understanding of interesting data the 'data list' command returns all metadata for images satisfying the provided filtering criteria.

    # search for NAIP data where the geohash starts with '9x'
    ./stip data search -p NAIP -g 9x -r 

    # search for data beginning on 2015-01-01 
    #   where the pixel coverage is greater than 95%
    ./stip data search -s 2524608000 -x 0.95

    # list all images from Sentinel-2 dataset for geohash '9xj3ej'
    ./stip data list -p Sentinel-2 -g 9xj3ej
#### DATA SPLIT
Images are stored at the geohash length defined during data loads. However, the 'data split' command enables further partitioning of datasets. This command launches a task on each cluster node to process data local to that machine. This command employs many of the same filtering criteria as 'data search' and 'data list' commands, enabling fine image processing filtering criteria.

    # split Sentinel-2 data at a geohash length of 6 
    #   for all geohashes starting with '9xj'
    ./stip data split -p Sentinel-2 -g 9xj -r -l 6
#### DATA FILL
Typically image datasets partition data into many tiles. The inherit tile bounds mean that often a single geohash spans multiple tiles. Therefore, when loading data, one image contains partial data whereas another contains the remaining data. The 'data fill' command attempts to identify image sets where 'complete' images may be built by combining multiple source images. This command launches a task on each cluster node to process data local to that machine. This command employs many of the same filtering criteria as 'data search' and 'data list' commands, enabling fine image processing filtering criteria.

    # attempt to fill all images for the NAIP dataset
    ./stip data fill -p NAIP

## TODO
- __data fill - fix v3.0__
- improve node logging
- __remove file descriptions in sqlite - vastly reduces memory use__
    - documentation on supported datasets
- refactor task implementations - facilitate code reuse
- st-image - on split, fill vectors with 'no_data_value'
