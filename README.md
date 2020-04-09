# STIP (SpatioTemporal Image Partitioner)
## OVERVIEW
A distributed spatiotemporal image management framework designed specifically for training neural networks.

## TODO
- data fill / split commands should query on an exact geohash - not include sub-geohashes
- abstract some task functionality - lots of code copies
- add 'min_coverage' field to 'data search' command
- instead of fill_all, serach_all, etc implement 'broadcast' message
- improve node logging
- image replication?
#### COMMANDS
- data split / merge command
- stop task functionality?
