#!/bin/python3

import argparse
import gdal
import multiprocessing
import os
import sys

# import realative 'stippy' python project
script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_dir + '/../../../stippy/')
import stippy

BANDS = [(0, 1), (0, 2), (0, 3), (0, 4), (1, 5)]

def compute_spectral_metrics(image):
    # compute max width and height
    width = 0
    height = 0
    for (file_index, band_index) in BANDS:
        gdal_dataset = gdal.Open(image.files[file_index].path)

        if gdal_dataset.RasterYSize > height:
            height = gdal_dataset.RasterYSize
        if gdal_dataset.RasterXSize > width:
            width = gdal_dataset.RasterXSize

    #print('image dimension: ' + str(width) + ' x ' + str(height))

    # compile array of image band reflectances
    band_array = []
    for i in range(0, height):
        band_array.append([])

        for j in range(0, width):
            band_array[i].append([])

    for (file_index, band_index) in BANDS:
        gdal_dataset = gdal.Open(image.files[file_index].path)
        array = gdal_dataset.GetRasterBand(band_index) \
            .ReadAsArray(buf_xsize=width, buf_ysize=height)
        #print('  ' + str(len(array[0])) + ', ' + str(len(array)))

        for i in range(0, height):
            for j in range(0, width):
                band_array[i][j].append(array[i][j] / 10000)
                #band_array[i][j].append(array[i][j])

    total_bsi = 0.0
    total_ndbi = 0.0
    total_ndvi = 0.0
    total_ndwi = 0.0
    for i in range(0, height):
        for j in range(0, width):
            # compute spectral metrics
            b02 = band_array[i][j][0]
            b03 = band_array[i][j][1]
            b04 = band_array[i][j][2]
            b08 = band_array[i][j][3]
            b11 = band_array[i][j][4]

            total_bsi += ((b11 + b04) - (b08 + b02)) \
                / ((b11 + b04) + (b08 + b02))
            total_ndbi += (b11 - b08) / (b11 + b08)
            total_ndvi += (b08 - b04) / (b08 + b04)
            total_ndwi += (b03 - b08) / (b03 + b08)

    total_pixels = width * height
    return (total_bsi / total_pixels), (total_ndbi / total_pixels), \
        (total_ndvi / total_pixels), (total_ndwi / total_pixels)

def process(image):
    # validate image
    if len(image.files) != 4:
        print('not all subdatasets found')
        return

    average_bsi, average_ndbi, average_ndvi, average_ndwi, \
        = compute_spectral_metrics(image)

    print("%8s %12d %8.4f %8.4f %8.4f %8.4f" % \
        (image.geocode, image.timestamp, average_bsi,
        average_ndbi, average_ndvi, average_ndwi))

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='compute image spectral metrics')
    parser.add_argument('album', type=str, help='stip album')
    parser.add_argument('-i', '--ip-address', type=str,
        help='stip host ip address', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int,
        help='stip host rpc port', default='15606')
    parser.add_argument('-t', '--thread-count', type=int,
        help='worker thread count', default='8')

    args = parser.parse_args()

    # compile list of processing images
    host_addr = args.ip_address + ':' + str(args.port)
    images = []
    for (node, image) in stippy.list_node_images(host_addr,
            args.album, min_pixel_coverage=0.9,
            max_cloud_coverage=0.2, platform='Sentinel-2'):
        images.append(image)

    # process images
    with multiprocessing.Pool(args.thread_count) as pool:
        pool.map(process, images)
