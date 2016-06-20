"""
Script for "fitting" various graph models to approximate OSN graphs

The Barabasi-Albert model does not have varied parameters. n = # of nodes, and m is set as |E|/n.

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

__author__ = "Adelbert Chang (adelbert_chang@cs.ucsb.edu), Alessandra Sala (alessandra@cs.ucsb.edu)"

__all__ = ['writeEdgeList',
           'getdk2',
           'fit_forestFire_mod',
           'fit_randomWalk_mod',
           'fit_nearestNeighbor_mod']

import os
import sys

import networkx as nx

import socialModels as sm
import twoKDistance as tk


def writeEdgeList(G, myFile):
    """
    Given a nx.Graph() as input, writes the edge list of the graph to a file.
    """
    outfile = open(myFile, 'w')
    for edge in G.edges_iter():
        outfile.write(str(edge[0]) + '\t' + str(edge[1]) + '\n')

    outfile.close()

def getdk2(G, graphID, dkPath, resultPath):
    """
    Extracts dK-2 distribution from G
    """
    # Get edge list from G
    edgeFile = resultPath + graphID + '_edgeList.txt'
    writeEdgeList(G, edgeFile)

    os.system(dkPath + ' -k 2 -i ' + edgeFile + ' > ' + resultPath + graphID + '_target.2k') 

def fit_forestFire_mod(graphSize, graphID, dkPath, original2k, resultPath):
    """
    Runs synthetic graph tests for various 'p' values (burn rate).
    """
    outfile = open(resultPath + graphID + '_ff_dkDistances.txt', 'w')
    
    p = 0.01
    while p < 1.0:
        print 'Running modified Forest Fire with parameters: n = ', graphSize, ' p = ', p
        
        newFile = graphID + '_ff_' + str(p)

        # Create synthetic graph
        syntheticGraph = sm.forestFire_mod(graphSize, p)

        # Write pickle, edge list, and 2k distro to file
        print 'Calculating dK-2...\n'
        getdk2(syntheticGraph, newFile, dkPath, resultPath)

        # Find distance between the dK-2 distributions
        dkDistance = tk.get_2k_distance(original2k, resultPath + newFile + '_target.2k')
        outfile.write(str(dkDistance) + '\tp = ' + str(p) + '\n')
        outfile.flush()

        p += 0.01

    outfile.close()

def fit_randomWalk_mod(graphSize, graphID, dkPath, original2k, resultPath, interval):
    """
    Runs synthetic graph tests for various 'qe' and 'qv' values
    If an interval was specified for fine grained sampling, please
    note that the interal is closed bounds, that is [x, y] so qe_end and qv_end
    will terminate after sampling the end values specified, not before.
    """
    if not interval:
        outfile = open(resultPath + graphID + '_rwCoarse_dkDistances.txt', 'w')
        qe = 0.1
        qe_end = 0.9
        step = 0.1

    else:
        outfile = open(resultPath + graphID + '_rwFine_dkDistances.txt', 'w')
        qe = interval[0] * 100
        qe_end = interval[1] * 100
        step = 1

    outfile.write('dk-2 Distance\tqe\tqv\n')

    while qe <= qe_end:
        if not interval:
            qv = 0.1
            qv_end = 0.9
        else:
            qv = interval[2] * 100
            qv_end = interval[3] * 100

        while qv <= qv_end:
            qeFloat = float(qe) / 100 if not interval else qe
            qvFloat = float(qv) / 100 if not interval else qv
            print 'Running modified Random Walk (coarse) with parameters: n = ', graphSize, ' qe = ', qeFloat, ' qv = ', qvFloat

            newFile = graphID + '_rwCoarse_' + str(qeFloat) + '_' + str(qvFloat)
            
            # Create synthetic graph
            syntheticGraph = sm.randomWalk_mod(graphSize, qeFloat, qvFloat)
            
            # Write pickle, edge list, and 2k distro to file
            print 'Calculating dK-2...\n'
            getdk2(syntheticGraph, newFile, dkPath, resultPath)
            
            # Find distance between the dK-2 distributions
            dkDistance = tk.get_2k_distance(original2k, resultPath + newFile + '_target.2k')
            outfile.write(str(dkDistance) + '\tqe = ' + str(qeFloat) + '\tqv = ' + str(qvFloat) + '\n')
            outfile.flush()

            qv += step

        qe += step

        outfile.write('\n')

    outfile.close()


def fit_nearestNeighbor_mod(graphSize, graphID, dkPath, original2k, resultPath, k):
    """
    Runs synthetic graph tests for various 'k' and 'u' values and stores them
    """
    if k:
        outfile = open(resultPath + graphID + '_nnFine_dkDistances.txt', 'w')
        kList = [k]
        uList = []
        myU = 0.01
        while myU < 1:
            uList.append(myU)
            myU += 0.01
        
    else:
        outfile = open(resultPath + graphID + '_nnCoarse_dkDistances.txt', 'w')
        kList = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        uList = [.01, .05, .10, .20, .25,.30, .35, .40, .45, .50,
                  .55, .60, .65, .70, .75,.80, .85, .95]

    outfile.write('dk-2 Distance\tk\tu\n')

    for k in kList:
        for u in uList:
            print 'Running modified Nearest Neighbor with parameters: n = ', graphSize, ' k = ', k, ' u = ', u
            
            newFile = graphID + '_nn_' + str(k) + '_' + str(u)

            # Create synthetic graph
            syntheticGraph = sm.nearestNeighbor_mod(graphSize, u, k)

            # Write pickle, edge list, and 2k distro to file
            print 'Calculating dK-2...\n'
            getdk2(syntheticGraph, newFile, dkPath, resultPath)

            # Find distance between the dK-2 distributions
            dkDistance = tk.get_2k_distance(original2k, resultPath + newFile + '_target.2k')
            outfile.write(str(dkDistance) + '\tk = ' + str(k) + '\tu = ' + str(u) + '\n')
            outfile.flush()

        outfile.write('\n')

    outfile.close()


if __name__ == "__main__":
    if len(sys.argv) < 6:
        sys.exit('Usage: python fitModel[_noPickling].py -ff/-rw/-nn <graph name> <pickle path> <path of dkDist code> <path of output folder> [additional parameters for modified Random Walk/Nearest Neighbor]')

    modelName = sys.argv[1]
    graphID = sys.argv[2]
    picklePath = sys.argv[3]
    dkPath = sys.argv[4]
    resultPath = sys.argv[5]

    if not os.path.exists('./' + resultPath):
        os.makedirs('./' + resultPath)

    print 'Extracting graph from pickle...'
    G = nx.read_gpickle(picklePath)

    # Extract dK-2
    print 'Extracting dK-2...'
    getdk2(G, graphID, dkPath, resultPath)

    # Begin fitting
    graphSize = len(G)

    if modelName == '-ff':
        fit_forestFire_mod(graphSize, graphID, dkPath, resultPath + graphID + '_target.2k', resultPath)

    elif modelName == '-rw':
        if len(sys.argv) < 7:
            print 'Modified random walk testing requires -coarse/-fine parameter as last argument'

        if sys.argv[6] == '-fine':
            if len(sys.argv) != 11:
                sys.exit('-fine flag requires additional qe_start qe_end qv_start qv_end parameters at the end.')

        # If coarse sampling, no intervals are needed
        if sys.argv[6] == '-coarse':
            interval = [] 

        # If fine sampling, make list of intervals in the order qe_start, qe_end, qv_start, qv_end
        elif sys.argv[6] == '-fine':
            qe_start = float(sys.argv[7])
            qe_end = float(sys.argv[8])
            qv_start = float(sys.argv[9])
            qv_end = float(sys.argv[10])
            interval = [qe_start, qe_end, qv_start, qv_end]
        else:
            sys.exit('Invalid -coarse/-fine value.')

        fit_randomWalk_mod(graphSize, graphID, dkPath, resultPath + graphID + '_target.2k', resultPath, interval)
            

    elif modelName == '-nn':
        if len(sys.argv) < 7:
            print 'Modified nearest neighbor testing requires -coarse/-fine parameter as last argument.'

        if sys.argv[6] == '-fine':
            if len(sys.argv) != 8:
                sys.exit('-fine flag requires additional k parameter at the end.')

        if sys.argv[6] == '-coarse':
            k = 0
        elif sys.argv[6] == '-fine':
            k = int(sys.argv[7])
        else:
            sys.exit('Invalid -coarse/-fine value.')

        fit_nearestNeighbor_mod(graphSize, graphID, dkPath, resultPath + graphID + '_target.2k', resultPath, k)
        
    else:
        sys.exit('Invalid parameters for -ff/-rw/-nn')
