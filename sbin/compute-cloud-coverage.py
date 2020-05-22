#!/bin/python3

import gdal
import math
import numpy as np
import pathlib
import s2cloudless
import sys

sys.path.append('../stippy/')
import stippy

BANDS = ['B01', 'B02', 'B04', 'B05',
    'B08', 'B8A', 'B09', 'B10', 'B11', 'B12']

def compute_cloud_coverage(directory, platform, geohash, source, tile):
    # compute max width and height
    width = 0
    height = 0
    for band in BANDS:
        path = directory + '/' + platform + '/' + geohash + '/' + band + '/' + source + '/' + tile
        gdal_dataset = gdal.Open(path)

        array = gdal_dataset.ReadAsArray()

        if len(array) > height:
            height = len(array)
        if len(array[0]) > width:
            width = len(array[0])

    #print('image dimension: ' + str(width) + ' x ' + str(height))

    # compile array of image band reflectances
    band_array = [[]]
    for i in range(0, height):
        band_array[0].append([])

        for j in range(0, width):
            band_array[0][i].append([])

    for band in BANDS:
        path = directory + '/' + platform + '/' + geohash + '/' + band + '/' + source + '/' + tile
        gdal_dataset = gdal.Open(path)

        array = gdal_dataset.ReadAsArray(buf_xsize=width, buf_ysize=height)

        #print('  ' + str(len(array[0])) + ', ' + str(len(array)))

        for i in range(0, height):
            for j in range(0, width):
                band_array[0][i][j].append(array[i][j] / 10000)

    # calculate cloud probability map
    cloud_detector = s2cloudless.S2PixelCloudDetector(all_bands=False)
    cloud_masks = cloud_detector.get_cloud_masks(np.array(band_array))

    # compute ratio of clear and cloud pixels
    cloud_pixels = 0
    clear_pixels = 0
    for i in range(0, height):
        for j in range(0, width):
            if cloud_masks[0][i][j] == 0:
                clear_pixels += 1
            else:
                cloud_pixels += 1

    #print(str(cloud_pixels) + ' ' + str(clear_pixels))
    return cloud_pixels / (cloud_pixels + clear_pixels)

if __name__ == "__main__":
    host_addr = '127.0.0.1:15606'

    image_iter = stippy.list_node_images(host_addr,
        platform='Sentinel-2A', band='TCI')

    images = []
    for (node, image) in image_iter:
        images.append(image)

    for image in images:
        path = pathlib.PurePath(image.path)
        print(image.geohash + ' ' + path.name)

        # compute cloud coverage percentage
        tile = path.name
        directory = str(path.parents[4])
        cloud_coverage = compute_cloud_coverage(directory,
            image.platform, image.geohash, image.source, tile)

        print('  cloud coverage: ' + str(cloud_coverage))
