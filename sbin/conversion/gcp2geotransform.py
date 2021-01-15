#!/bin/python3

import sys
from osgeo import gdal, gdal_array

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: " + sys.argv[0] 
            + " <latitude-dataset> <longitude-dataset> FILENAMES...")
        sys.exit(1)

    # read latitude and longitude arrays
    latarray = gdal_array.LoadFile(sys.argv[1])
    longarray = gdal_array.LoadFile(sys.argv[2])

    # compile collection of glocal control points
    gcps=[]
    for i in range(0, len(latarray), 5):
        for j in range(0, len(latarray[i]), 5):
            gcp = gdal.GCP(float(longarray[i][j]),
                float(latarray[i][j]), 0.0, i, j)

            gcps.append(gcp)

    # compute geotransform from gcps
    geotransform = gdal.GCPsToGeoTransform(gcps)

    # set geotransform on datasets
    for i in range(3, len(sys.argv)):
        dataset = gdal.Open(sys.argv[i], gdal.GA_Update)
        dataset.SetGeoTransform(geotransform)
