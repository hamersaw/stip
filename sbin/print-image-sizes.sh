#!/bin/bash
# ./sbin/print-image-sizes.sh ~/Downloads/STIP-6/ TCI \
#    | awk '{ xsum += $2; ysum+=$3; n++ } END { if (n > 0) print xsum / n, ysum/n; }'

# check arguments
if (( $# < 1 )); then
    echo "usage: $(basename $0) directory [band]"
    exit
fi

band="*"
if (( $# == 2 )); then
    band=$2
fi

# iterate over files
for file in $(find $1 -type f -wholename "*/$band/*/*tif"); do
    #echo $file

    # identify image band
    array=($(echo $file | tr "/" "\n"))
    band=${array[-3]}

    # identify image dimensions
    dimensions=$(gdalinfo $file | grep "Size is" \
        | awk '{print $3,$4}' | awk -F, '{print $1,$2}')
    x_dimension=$(echo $dimensions | awk '{print $1}')
    y_dimension=$(echo $dimensions | awk '{print $2}')

    echo $band $x_dimension $y_dimension
done
