#!/bin/python3

import argparse
import gdal
import math
import multiprocessing
import numpy as np
import os
import pathlib
import s2cloudless
import sys

# import realative 'stippy' python project
script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_dir + '/../../stippy/')
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

def process(image):
    # compute path of image
    path = pathlib.Path(image.path)

    # compute cloud coverage percentage
    tile = path.name
    directory = str(path.parents[4])
    cloud_coverage = compute_cloud_coverage(directory,
        image.platform, image.geohash, image.source, tile)

    print(image.geohash + ' ' + path.name
        + ' ' + str(cloud_coverage))

    # update all existing image bands
    for path in path.parents[2].glob('*/' + image.source + '/' + tile):
        gdal_dataset = gdal.Open(str(path))
        gdal_dataset.SetMetadataItem("CLOUD_COVERAGE",
            str(cloud_coverage), "STIP")

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='compute cloud coverage')
    parser.add_argument('-i', '--ip-address', type=str,
        help='stip host ip address', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int,
        help='stip host rpc port', default='15606')
    parser.add_argument('-t', '--thread-count', type=int,
        help='worker thread count', default='4')

    args = parser.parse_args()

    # compile list of processing images
    host_addr = args.ip_address + ':' + str(args.port)
    image_iter = stippy.list_node_images(host_addr,
        platform='Sentinel-2A', band='TCI')
    images = []

    for (node, image) in stippy.list_node_images(host_addr,
            platform='Sentinel-2A', band='TCI'):
        images.append(image)

    for (node, image) in stippy.list_node_images(host_addr,
            platform='Sentinel-2B', band='TCI'):
        images.append(image)

    # process images
    with multiprocessing.Pool(args.thread_count) as pool:
        pool.map(process, images)
