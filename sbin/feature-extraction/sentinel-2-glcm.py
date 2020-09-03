#!/bin/python3

import argparse
import multiprocessing
import numpy as np
import os
from skimage import color, io, util
from skimage.feature import greycomatrix, greycoprops
import sys

# import realative 'stippy' python project
script_dir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(script_dir + '/../../../stippy/')
import stippy

def process(image):
    # read in grayscale image
    img = io.imread(image.files[3].path, as_gray=True)
    ubyte_img = util.img_as_ubyte(img)

    # calculate gray level cooccurance matrix
    distances = [1, 2, 3]
    #angles = [0, np.pi/2]
    angles = [0, np.pi/4, np.pi/2, 3*np.pi/4]
    matrix = greycomatrix(ubyte_img, distances=distances, \
        angles=angles,symmetric=True, normed=True)

    # calculate gray cooccurance properties
    contrast = greycoprops(matrix, 'contrast').mean(axis=1)
    dissimilarity = greycoprops(matrix, 'dissimilarity').mean(axis=1)
    homogeneity = greycoprops(matrix, 'homogeneity').mean(axis=1)
    energy = greycoprops(matrix, 'energy').mean(axis=1)
    correlation = greycoprops(matrix, 'correlation').mean(axis=1)
    asm = greycoprops(matrix, 'ASM').mean(axis=1)

    data = []
    data.extend(contrast)
    data.extend(dissimilarity)
    data.extend(homogeneity)
    data.extend(energy)
    data.extend(correlation)
    data.extend(asm)

    print(image.geocode + ' ' + str(data))

if __name__ == "__main__":
    # parse arguments
    parser = argparse.ArgumentParser(description='compute grey level co-occurance matrix (glcm) features')
    parser.add_argument('album', type=str, help='stip album')
    parser.add_argument('date', type=int, help='target date')
    parser.add_argument('-i', '--ip-address', type=str,
        help='stip host ip address', default='127.0.0.1')
    parser.add_argument('-p', '--port', type=int,
        help='stip host rpc port', default='15606')
    parser.add_argument('-t', '--thread-count', type=int,
        help='worker thread count', default='4')

    args = parser.parse_args()

    # compile list of processing images
    host_addr = args.ip_address + ':' + str(args.port)
    images = {}
    for (node, image) in stippy.list_node_images(host_addr,
            args.album, platform='Sentinel-2',
            min_pixel_coverage=0.95, max_cloud_coverage=0.05):
        # skip images that are missing a subdataset
        if len(image.files) != 4:
            continue

        if image.geocode not in images:
            # if geocode has not yet been processed -> add image
            images[image.geocode] = image
        elif abs(images[image.geocode].timestamp - args.date) \
                > abs(image.timestamp - args.date):
            # if image is closer to target date -> add image
            images[image.geocode] = image

    # process images
    with multiprocessing.Pool(args.thread_count) as pool:
        pool.map(process, images.values())
