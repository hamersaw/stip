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
stipd is the cluster node application.
#### START CLUSTER
TODO
#### STOP CLUSTER
TODO
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
