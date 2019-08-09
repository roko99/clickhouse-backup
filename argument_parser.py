#!/usr/bin/env python3
# include standard modules
import argparse

#verbose = False
# initiate the parser
parser = argparse.ArgumentParser()
parser.add_argument("-v", "--verbose", help="show program tracebacks", action="store_true")
parser.add_argument("-f", "--from", help="set source host", dest='ffrom')
parser.add_argument("-t", "--to", help="set destination host")

args = parser.parse_args()

if args.ffrom:
    srchost = args.ffrom
if args.to:
    dsthost = args.to

print(srchost)
