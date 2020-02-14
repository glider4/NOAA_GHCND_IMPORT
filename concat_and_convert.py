# -*- coding: utf-8 -*-
"""
Import NOAA GHCN-D data

Concat all files in tarball to a single, 30+ GB .txt,
then use fixed-width read via Pandas to convert to one giant .csv

"""

import pandas as pd
import glob

''' Change paths to your local destination for extracted tarball files '''

# Concat all .dly files in directory together
# Credit: https://stackoverflow.com/questions/17749058/

read_files = glob.glob("D:/ghcnd_all/*.dly")

with open("D:/result.txt", "wb") as outfile:
    for f in read_files:
        with open(f, "rb") as infile:
            outfile.write(infile.read())

# OBS Data - 31 days (if less than read null) of 4 columns each, plus initial 4 info columns
# Credit: https://stackoverflow.com/questions/45870220
# For each chunk of data, convert to CSV and append into single file 'ghcnd-all.csv'
            
for chunk in pd.read_fwf('D:/result.txt',
                widths=[11,4,2,4,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1
                        ,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1
                        ,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1,5,1,1,1],
                        header=None, chunksize=15000):
    
    chunk.to_csv('D:/ghcnd-all.csv',
                       mode='a', sep=',', encoding='utf-8', header=None)