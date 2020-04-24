# STIP (SpatioTemporal Image Partitioner)
## OVERVIEW
A distributed spatiotemporal image management framework designed specifically for training neural networks.

## TODO
- change 'base' dataset to 'raw'?
- add LZW compression on GeoTiff files
- instead of using .meta file -> add metadata attribute to GeoTiff
- use LoadFormat on LoadEarthExplorerTask
- data fill / split commands should query on an exact geohash - not include sub-geohashes
- abstract some task functionality - lots of code copies
- add 'min_coverage' field to 'data search' command
- instead of fill_all, search_all, etc implement 'broadcast' message
- improve node logging
- image replication?
#### COMMANDS 
- data fill - fix
- data merge
- data search - fix
- data split - fix
- stop task functionality?
