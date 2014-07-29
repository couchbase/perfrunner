"""
Helper script for model fitting (fitModels[_noPicking].py) that finds distance between two dK-2 distributions

References
-----------
[1] Sala A., Cao L., Wilson, C., Zablit, R., Zheng, H., Zhao, B. Measurement-calibrated graph models for social network experiments. In Proc. of WWW (2010).

"""
#    Copyright (C) 2011 by
#    Alessandra Sala <alessandra@cs.ucsb.edu>
#    Lili Cao <lilicao@cs.ucsb.edu>
#    Christo Wilson <bowlin@cs.ucsb.edu>
#    Robert Zablit <rzablit@cs.ucsb.edu>
#    Haitao Zheng <htzheng@cs.ucsb.edu>
#    Ben Y. Zhao <ravenben@cs.ucsb.edu>
#    All rights reserved.
#    BSD license.

__author__ = "Alessandra Sala (alessandra@cs.ucsb.edu), Adelbert Chang (adelbert_chang@cs.ucsb.edu)"

__all__ = ['comparePairs',
           'get_2k_distance']

import string
import math

def comparePairs(x11, x12, x21, x22):
    """
    Helper function for get_2k_distance
    """
    if x11 > x21:
        return True
    elif x11 < x21:
        return False
    elif x12 > x22:
        return True
    elif x12 < x22:
        return False
    return False

def get_2k_distance(file1, file2):
    """
    Find distance between two dK-2 distributions
    """
    # Open files..
    infile1 = open(file1)
    infile2 = open(file2)

    # Read/compare each line from each file one at a time
    line1 = infile1.readline()
    line2 = infile2.readline()
    words1 = string.split(line1)
    words2 = string.split(line2)

    distance = 0
    while line1 and line2:
        # If there are not 3 values on that line, move on
        # If it is empty, the while condition will catch it.
        # We expect 3 values because dK-2 distributions are formatted as:
        # <deg1> <deg2> <number of edges>
        if len(words1) != 3:
            line1 = infile1.readline()
            words1 = string.split(line1)
            continue

        if len(words2) != 3:
            line2 = infile2.readline()
            words2 = string.split(line2)
            continue

        # Find/accumulate distance measure as necessary
        if comparePairs(string.atoi(words1[0]), string.atoi(words1[1]), string.atoi(words2[0]), string.atoi(words2[1])):
            distance += string.atoi(words2[2]) ** 2
            line2 = infile2.readline()
            words2 = string.split(line2)
            continue

        if comparePairs(string.atoi(words2[0]), string.atoi(words2[1]), string.atoi(words1[0]), string.atoi(words1[1])):
            distance += string.atoi(words1[2]) ** 2
            line1 = infile1.readline()
            words1 = string.split(line1)
            continue

        distance += (string.atoi(words1[2]) - string.atoi(words2[2])) ** 2

        # Get next lines
        line1 = infile1.readline()
        words1 = string.split(line1)
        
        line2 = infile2.readline()
        words2 = string.split(line2)

    while line1:
        if len(words1) == 3:
            distance += string.atoi(words1[2]) ** 2
        line1 = infile1.readline()
        words1 = string.split(line1)

    while line2:
        if len(words2) == 3:
            distance += string.atoi(words2[2]) ** 2
        line2 = infile2.readline()
        words2 = string.split(line2)

    return distance ** 0.5 # Square root
