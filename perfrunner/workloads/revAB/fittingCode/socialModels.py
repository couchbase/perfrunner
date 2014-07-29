"""
Generator for online social network (OSN) models, v0.5, Updated 10/21/11

References
-----------
[1] Sala A., Cao L., Wilson, C., Zablit, R., Zheng, H., Zhao, B. Measurement-calibrated graph models for social network experiements. In Proc. of WWW (2010).

The paper describes 6 social graph models proposed to use as models to replace social graphs:
Barabasi-Albert, Forest Fire (modified), Random Walk (modified), Nearest Neighbor (modified), Kronecker Graphs, and dK-2

A Barabasi-Albert generator has already been implemented for NetworkX as networkx.generators.random_graphs.barabasi_albert_graph(n, m, create_using = None, seed = None)
A dK-2 generator implemented in C++ can be found at http://www.sysnet.ucsd.edu/~pmahadevan/topo_research/topo.html
The Kronecker generator can be found in the C++ SNAP library, found at http://snap.stanford.edu/snap/download.html

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

__author__ = 'Lili Cao (lilicao@cs.ucsb.edu), Adelbert Chang (adelbert_chang@cs.ucsb.edu)'

__all__ = ['forestFire_mod_burnProcedure',
           'forestFire_mod',
           'randomWalk_mod',
           'nearestNeighbor_mod']

import sys
import networkx as nx
import random
import math
import bisect

# Control recursion depth
global limit
sys.setrecursionlimit(500000)

def forestFire_mod_burnProcedure(G, node1, node2, myDict, p):
    """
    Helper function for forestFire_mod
    Handles burning procedure recursively for the modified Forest Fire model
    Recurive depth is handled by global variable 'limit'
    """
    # Ensure recursion does not go too deep
    global limit
    limit += 1

    if limit >= 5000:
        return

    # Generate a non-zero random floating point between 0 and 1
    y = 0
    while y == 0:
        y = random.random()

    # How many neighbors to "burn"
    x = (int)(math.ceil((math.log10(y) / math.log10(p)) - 1))
    burn = [] # Keep track of which neighbors to burn
    nbors = G.neighbors(node1)

    # No neighbors to burn
    if len(nbors) == 0:
        return G

    # If there are fewer neighbors than needed to burn, burn all
    elif len(nbors) <= x:
        for i in range(0, len(nbors)):
            if not myDict.has_key(nbors[i]):
                burn.append(nbors[i])
                myDict[nbors[i]] = nbors[i]

    # Choose the 'x' amount of neighbors to burn
    else:
        for i in range(0, x):
            a = random.randrange(0, len(nbors))
            b = 0
            for j in range(0, i):
                while nbors[a] == burn[j] or myDict.has_key(nbors[a]):
                    a = random.randrange(0, len(nbors))
                    if myDict.has_key(nbors[a]):
                        b += 1
                    if (len(nbors) - b) < x:
                        break

                if (len(nbors) - b) < x:
                    break

            if (len(nbors) - b) < x:
                break

            burn.append(nbors[a])
            myDict[nbors[a]] = nbors[a]

    # Burn
    for i in range(0, len(burn)):
        if burn[i] != node2:
            G.add_edge(node2, burn[i])

    # Repeat recursively
    for i in range(0, len(burn)):
        forestFire_mod_burnProcedure(G, burn[i], node2, myDict, p)

def forestFire_mod(n, p):
    """
    Generates a graph based on a modified Forest Fire model.

    This is a modified version of the Forest Fire model
    that creates undirected edges in the edge creation process.[1]

    Input:
        n = number of nodes in the graph (integer)
        p = "burn" rate (floating point)
    
    Output:
        nx.Graph()
    """
    # Prepare graph
    G = nx.Graph()
    nodeCounter = 0 # Tracks next available node ID

    # Keep adding nodes until we have 'n' nodes
    while nodeCounter < n:
        # Recursion limit
        global limit
        limit = 0
        target = nodeCounter
        G.add_node(target)
        nodeCounter = len(G) # Update next available nodeID
        
        if nodeCounter == 1:
            continue
        
        # Select a random node from graph that is not the current 'target' node
        randomNode = random.randrange(0, nodeCounter)
            
        myDict = dict()
        G.add_edge(randomNode, target)
        myDict[randomNode] = randomNode

        # Start burning
        forestFire_mod_burnProcedure(G, randomNode, target, myDict, p)

    return G
        
    
def randomWalk_mod(n, qe, qv):
    """
    Generates a graph based on a modified Random Walk  model.

    This is a modified version of the Random Walk model
    that creates undirected edges in the edge creation process.[1]

    Input:
        n = number of nodes in the graph (integer)
        qe = probability of continuing the walk after each step (floating point)
        qv = probability of attaching to a visited node (floating point)
    Output:
        nx.Graph()
    """
    # Prepare graph..
    G = nx.Graph()
    
    # Keep track of next available node ID
    nodeCounter = 0

    while nodeCounter < n:
        target = nodeCounter
        G.add_node(target) # Add node to graph
        nodeCounter += 1

        if target == 0: # If only one node, move to next step.
            continue

        walkedNode = random.randrange(0, target)
        
        G.add_edge(target, walkedNode)

        while True:
            # With probability 'qv' attach to the node we're "walked on"
            if random.random() <= qv:
                if walkedNode != target:
                    G.add_edge(target, walkedNode)

            # With probability 'qe', continue the walk
            if random.random() <= qe:
                if len(G.neighbors(walkedNode)) > 0:
                    walkedNode = random.sample(G.neighbors(walkedNode), 1)[0]

            else:
                break

    return G

def nearestNeighbor_mod(n, u, k):
    """
    Generates a graph based on a modified Nearest Neighbor  model.

    This is a modified version of the Nearest Neighbor model
    that creates an undirected graph with power-law exponent
    between 1.5 and 1.75, matching that of online social networks.
    This is done so that each time a new node is added, k random
    pairs of nodes in the connected component of the graph are connected. [1]

    Input:
        n = number of nodes in the graph (integer)
        u = probability that determines if a new node is added or if a pair of 2 hop neighbors is connected (floating point)
        k = each time a new node is added, k pairs of random nodes in the connected component are connected (integer)
    Output:
        nx.Graph()
    """
    G = nx.Graph()
    nodeCounter = 0 # Keeps track of ID of next available node

    degreeArray = [0 for i in range(0,n)]
    d = []
    N = [0 for i in range(0,2)]

    while nodeCounter < n: # Until we reach 'n' nodes...
        if random.random() < u:
            if len(d) == 0:
                continue

            x = random.choice(d) # Pick a node from list
            N = random.sample(G.neighbors(x), 2) # Pick 2 unique nodes in the list of neighbors

            if not G.has_edge(N[0], N[1]): # If no edge exists between the 2, connect
                G.add_edge(N[0], N[1])
                degreeArray[N[0]] += 1
                degreeArray[N[1]] += 1
                if degreeArray[N[0]] == 2:
                    bisect.insort(d, N[0])

                if degreeArray[N[1]] == 2:
                    bisect.insort(d, N[1])

        else:
            nodesSoFar = nodeCounter
            G.add_node(nodeCounter)
            nodeCounter += 1

            if nodeCounter == 1: # No use in continuing if there is only one node in the graph
                continue

            a = random.randrange(0, nodesSoFar) # Pick a node in the graph
            G.add_edge(a, nodesSoFar)
            degreeArray[a] += 1
            degreeArray[nodesSoFar] += 1

            if degreeArray[a] == 2:
                bisect.insort(d, a)
            if degreeArray[nodesSoFar] == 2:
                bisect.insort(d, nodesSoFar)

            for i in range(0, k): # Connect k random pairs in the graph
                N[0] = random.randint(0, nodeCounter - 1)
                N[1] = N[0]

                while N[1] == N[0]: # Ensure the two nodes are different (no self-loops)
                    N[1] = random.randint(0, nodeCounter - 1)

                if not G.has_edge(N[0], N[1]):
                    G.add_edge(N[0], N[1])

                    degreeArray[N[0]] += 1
                    degreeArray[N[1]] += 1

                    if degreeArray[N[0]] == 2:
                        bisect.insort(d, N[0])
                    if degreeArray[N[1]] == 2:
                        bisect.insort(d, N[1])

    return G
