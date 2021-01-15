#!/bin/bash

# TODO - check for gdal_translate and gdal_merge.py

# check arguments
if [ $# != 1 ]; then
    echo "usage: $(basename $0) <filename>"
    exit
fi

# initialize global variables
tmpdir="/tmp"

collections=( "01 03 05 07 09 12 13" "02 04 06 08 10 11" )
latsubdataset=14
longsubdataset=15

tilename=$(basename ${1%.*})
directory=$(dirname $1)

# split file on subdataset
gdal_translate -sds $1 "$tmpdir/$tilename.tif" >/dev/null

# merge subdataset files for each collection
count=0
filenames=""
for collection in "${collections[@]}"; do
    # compile filenames for merged file
    mergefilenames=""
    for subdataset in $collection; do
        if [ -n "$mergefilenames" ]; then
            mergefilenames="$mergefilenames "
        fi

        mergefilenames+="$tmpdir/$tilename\_$subdataset.tif"
    done

    # merge subdatasets
    filename="$directory/$tilename-$count.tif"
    gdal_merge.py -o "$filename" -separate $mergefilenames >/dev/null

    if [ -n "$filenames" ]; then
        filenames="$filenames "
    fi

    filenames+="$filename"

    # cleanup
    rm $mergefilenames

    # increment subdataset count
    count=$(( $count + 1 ))
done

# sample global control points to set geotransform
projectdir=$(dirname $0)
python3 $projectdir/gcp2geotransform.py \
    $tmpdir/$tilename\_$latsubdataset.tif \
    $tmpdir/$tilename\_$longsubdataset.tif $filenames

rm $tmpdir/$tilename\_$latsubdataset.tif \
    $tmpdir/$tilename\_$longsubdataset.tif
