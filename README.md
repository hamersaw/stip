# stip (SpatioTemporal Image Partitioner)
## OVERVIEW
A distributed spatiotemporal image management framework designed specifically for training neural networks.

## TODO
- add Filter protobuf -> use everywhere
- image replication? - one replica on geohash of length (x - 1)
- improve node logging
- __multithread image loading - cpu usage is very low__
- open listening sockets on 0.0.0.0?
- refactor task implementations - facilitate code reuse
#### COMMANDS 
- __data load - support MODIS data__
- data merge - combine images into higher level images
- **cloud coverage - computation on images**
- task stop?
