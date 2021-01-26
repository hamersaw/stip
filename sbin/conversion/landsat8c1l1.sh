#!/bin/bash

# TODO - check for gdalinfo, gdal_translate, and gdal_merge.py

# check arguments
if [ $# != 1 ]; then
    echo "usage: $(basename $0) <filename>"
    exit
fi

# initialize global variables
tmpdir="/tmp"
collections=( "1 2 3 4 5 6 7 9 10 11 QA" "8" )

# parse metadata
tilename=$(basename "$1" | cut -f 1 -d '.')
directory=$(dirname $1)

datestring=${tilename:17:8}
if [ -z "$datestring" ]; then
    echo "failed to identify date string"
    exit
fi

# decompress tar archive
tar xvf $1 -C $tmpdir > /dev/null

# merge subdataset files for each collection
count=0
for collection in "${collections[@]}"; do
    # compile filenames for merged file
    mergefilenames=""
    for subdataset in $collection; do
        if [ -n "$mergefilenames" ]; then
            mergefilenames="$mergefilenames "
        fi

        mergefilenames+="$tmpdir/$tilename\_B$subdataset.TIF"
    done

    # merge subdatasets
    filename="$directory/$tilename-$count.tif"
    gdal_merge.py -o "$filename" -separate $mergefilenames >/dev/null

    # set metadata
    gdal_edit.py -mo "PLATFORM=Landsat8C1L1" -mo "SUBDATASET=$count" \
        -mo "TILE=$tilename" -mo "TIMESTAMP=$datestring" "$filename"

    # increment subdataset count
    count=$(( $count + 1 ))
done

# cleanup
rm $tmpdir/$tilename*
