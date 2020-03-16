#!/bin/python3

import sys

MIN = 0
MAX = 18446744073709551615

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: " + sys.argv[0] + " <node-count> <token-count>")
        sys.exit(1)

    # parse arguments
    nodes = int(sys.argv[1])
    tokens = int(sys.argv[2])
    
    # compute token delta
    token_count = nodes * tokens
    token_delta = (MAX - MIN) / token_count 

    # generate tokens
    for i in range(nodes):
        for j in range(tokens):
            token = (token_delta * nodes * j) + (token_delta * i) + MIN
            sys.stdout.write(" -t " + str(int(token)))

        sys.stdout.write('\n')
