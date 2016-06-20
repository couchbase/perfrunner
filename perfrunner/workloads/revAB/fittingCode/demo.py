"""
Demo/walkthrough script for synthetic graph generation and fitting.

Last modified: Nov 15, 2011 @ 6:37pm PST

If you have problems/questions/comments/concerns, email:
adelbert_chang@cs.ucsb.edu
"""

#    Copyright (C) 2011 by
#    Adelbert Chang <adelbert_chang@cs.ucsb.edu>

__author__ = 'Adelbert Chang (adelbert_chang@cs.ucsb.edu)'

import os
import sys


def prompt_user():
    raw_input('(Hit Enter to continue)')

print 'Welcome to a demo of our OSN synthetic generation/fitting code.\n' \
    'Before we continue, please make sure you have Python 2.7, ' \
    'NetworkX, and Orbis installed.'
print 'It is also recommended that you run this demo in a GNU Screen ' \
    ' on a dedicated server, as parts of this demo may take a while to run, ' \
    'depending on the machine. Additionally, you may first need to modify ' \
    'the socialModels.py, fitModel.py, and fitModel_noPickling.py scripts by ' \
    'adding a\nsys.path.append(\'where your NetworkX library is located as a string\')' \
    '\nin front of "import sys" and before "import networkx as nx".)\n\n'
print 'If you have any problems/questions/comments/concerns, ' \
    'please email adelbert_chang@cs.ucsb.edu.'
prompt_user()

print '\nLet\'s start things off by first generating a synthetic graph...\n'
print 'I am going to generate a synthetic graph based on the modified ' \
    'Nearest Neighbor model, with parameters I have previously fitted ' \
    'for one of our 2008 Facebook graphs.'
prompt_user()

print '\nI can do this by using the socialModels.py file included in the tar ' \
    'as a module. I will now create a Python script called demo_makeGraph.py ' \
    'to generate a Python .pickle of this synthetic graph.'
prompt_user()

promptText = '\nPlease type in the full path of where your NetworkX library is' \
    ', or 0 if it is already part of your Python path.:\n'
nxPath = raw_input(promptText)
print '\nThank you. Creating script...'
outfile = open('demo_makeGraph.py', 'w')
if nxPath == '0':
    myCode = "import sys\n" \
        "import networkx as nx\nimport socialModels as sm\n" \
        "G = sm.nearestNeighbor_mod(6117, 1, .82)\n" \
        "nx.write_gpickle(G, 'demo_Graph.pickle')\n"
else:
    myCode = "import sys\nsys.path.append('" + nxPath + "')\n" \
        "import networkx as nx\nimport socialModels as sm\n" \
        "G = sm.nearestNeighbor_mod(6117, .82, 1)\n" \
        "nx.write_gpickle(G, 'demo_Graph.pickle')\n"
outfile.write(myCode)
outfile.close()
print 'Script has been created. I will now run this script to generate ' \
    'a Python NetworkX .pickle file called "demo_Graph.pickle" which stores our graph data.'
prompt_user()

print '\nRunning demo_makeGraph.py (this may take a while) ...\n'
os.system('python demo_makeGraph.py')
print 'Pickle has been created as demo_Graph.pickle.\n'

###########

print '-------------------------\n'

print 'I will now demonstrate how to run our fitting code, using our newly generated ' \
    'demo_Graph.pickle as the target.'
prompt_user()

print '\nI have included two versions of our fitting code with the download: ' \
    'fitModel.py and fitModel_noPickling.py. The only difference between these two ' \
    'file is that fitModel.py will generate a Python NetworkX .pickle file for every ' \
    'single synthetic graph that is generated during the parameter varying process. ' \
    'This is usually not needed, and running fitModel_noPickling.py instead will not ' \
    'generate these pickle files. The command line arguments for both are the same.' \
    'In this demo, I will be using fitModel_noPickling.py.'
prompt_user()

print '\nIn this demo, I will use our modified Random Walk model as an example on fitting ' \
    'a graph model to a real OSN (online social network) graph. The command line arguments ' \
    'that will be shown are specific to the modified Random Walk model. Arguments for ' \
    'other models will be mentioned up ahead.'
prompt_user()

dkPath = raw_input( '\nPlease type in the full path of where the dkDist program inside Orbis is, including the program name i.e. /code/Orbis_Source/dkDist :\n')
print '\nThank you. For modified Forest Fire, the first command line argument would be as follows:\n' \
    'python fitModel_noPickling.py -rw demoOSN ./demo_Graph.pickle ' \
    + dkPath + ' ./demo_results/ -coarse\n'
print 'The additional "-coarse" parameter at the end is required for both the modified Random ' \
    'Walk model and the modified Nearest Neighbor model during this initial fitting phase. The ' \
    'modified Forest Fire model does not need this additional parameter.'
prompt_user()

print '\nThe above code, when run, will begin "fitting" for the graph/pickle located at ./demo_Graph.pickle. ' \
    'It will create several edge lists and dK-2 distributions inside the folder ./demo_results/. ' \
    'Each file will lead with "demoOSN" (this name could be anything) followed by the parameter values ' \
    'of that specific synthetic graph. In our example, an example of such files would look like:\n' \
    'demoOSN_rwCoarse_0.1_0.1_edgeList.txt and demoOSN_rwCoarse_0.1_0.1_target.2k'
prompt_user()

print '\nPerhaps the most important of these files has the suffix *_dkDistances.txt. This file lists ' \
    'the distance of all the synthetic graph\'s dK-2 distribution from the target graph\'s. We will ' \
    'use this later on to define the parameters for fine fitting after coarse fitting is complete.\n'
print 'I will now execute the fitting code with the command described above. This may take a while.'
prompt_user()

print '\nRunning coarse fitting (this may take a while)...'
os.system('python fitModel_noPickling.py -rw demoOSN ./demo_Graph.pickle ' + dkPath + ' ./demo_results/ -coarse')

print '\n\nCoarse fitting has finished! We now go into fine fitting. ' \
    'Please go into the ./demo_results/ folder and open the demoOSN_rwCoarse_dkDistances.txt ' \
    'file, and note the "qe" and "qv" value which has the minimal dk-2 distance.' \
    '(Note: If in the future you are fitting for modified Forest Fire, you need only note the parameters ' \
    'after the first run. These next few steps do not apply to the modified Forest Fire model.)'
prompt_user()

interval = raw_input('\nPlease enter the optimal qe and qv values, separated by a space: ')
interval = interval.split()
qe = interval[0]
qv = interval[1]
print '\nThank you. Note that for the modified Nearest Neighbor model, you would want to look for just the ' \
    'optimal "k" value and use that in place of "qe" and "qv" in this next step.\n'
fineParameters = qe + ' ' + str(float(qe) + 0.09) + ' ' + qv + ' ' + str(float(qv) + 0.09)
print 'To run fine fitting, we execute the same command as before, but with some new parameters at the end:\n' \
    'python fitModel_noPickling.py -rw demoOSN ./demo_Graph.pickle ' + dkPath + ' ./demo_results/ -fine ' \
    + fineParameters + '\n'
print 'Note the additional qe/qv + .09 values. These four values indicate the intervals of qe and qv to ' \
    'sample over. The format is qe_start qe_end qv_start qv_end. Of course, you may choose to specify your own ' \
    'interval, but I usually use the +.09 interval.'
prompt_user()

print '\nI am now going to run the above fine fitting code.'
prompt_user()

print 'Running fine fitting (this may take a while)...'
os.system('python fitModel_noPickling.py -rw demoOSN ./demo_Graph.pickle ' + dkPath + ' ./demo_results/ -fine ' + fineParameters)

print '\n\nFine fitting has finished! Now all that is left to do is to look into the ./demo_results/ folder for the ' \
    'demoOSN_rwFine_dkDistances.txt file, and find the optimal qe/qv values. Those are your parameters.\n'
print 'This concludes the demo. If you have any problems/questions/comments/concerns, please contact: ' \
    'adelbert_chang@cs.ucsb.edu. Have a nice day!\n'
