# STIP (SpatioTemporal Image Partitioner)
## OVERVIEW
A distributed spatiotemporal image management framework designed specifically for training neural networks.

## TODO
- add LZW compression on GeoTiff files
- instead of using .meta file -> add metadata attribute to GeoTiff
- use LoadFormat in LoadEarthExplorerTask
- data fill / split commands should query on an exact geohash - not include sub-geohashes
- test 'data fill' command
- abstract some task functionality - lots of code copies
- add 'min_coverage' field to 'data search' command
- instead of fill_all, search_all, etc implement 'broadcast' message
- improve node logging
- image replication?
#### COMMANDS 
- compute cloud coverage on images
- data merge
- stop task functionality?
