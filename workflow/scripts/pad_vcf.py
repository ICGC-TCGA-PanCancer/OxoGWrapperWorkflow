#! /usr/bin/python3

"""
This script will right-pad the lines in a VCF to ensure
that they are all the same length as the longest line. 

Usage:
$ python3 pad_vcf.py file.vcf > padded_file.vcf

Note: This script is assuming that the header line has the most fields (tab characters). 
If there are other lines that are longer than the header than the output of this may not be correct.
"""

import sys

with open(sys.argv[1]) as vcf:
    max_tabs = 0
    for line in vcf:
        num_tabs = line.count('\t')
        if num_tabs >= max_tabs:
            max_tabs = num_tabs
            print(line.replace('\n',''))
        else:
            suffix = "".join("\t." for x in range(max_tabs - num_tabs))
            print(line.replace('\n','') + suffix )
