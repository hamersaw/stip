# stip (SpatioTemporal Image Partitioner)
## OVERVIEW
A distributed spatiotemporal image management framework designed specifically for training neural networks.

## TODO
- image replication? - one replica on geohash of length (x - 1)
- improve node logging
- __multithread image loading - cpu usage is very low__
- open listening sockets on 0.0.0.0?
#### COMMANDS 
- __data load - support MODIS data__
- data merge - combine images into higher level images
- **data search - separate function to leverage SQL**
- **cloud coverage - computation on images**
- task stop?
#### REFACTOR
- image search
- task implementation
