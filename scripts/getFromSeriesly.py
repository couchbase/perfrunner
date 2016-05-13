from seriesly import Seriesly
import sys
from collections import OrderedDict


host = sys.argv[1]
stat = sys.argv[2]
if len(sys.argv) > 3:
   field = sys.argv[3]
else:
   field = None

seriesly = Seriesly(host=host, port=3133)

s=seriesly[stat].get_all()   # Q1 bad result
#s=seriesly['n1ql_statsperfregression_450-2151-enterprise_fae'].get_all()   # Q1 good result
#s=seriesly['n1ql_statsperfregression_450-2151-enterprise_a36'].get_all()    # good resu;t Q3 stale ok
#s=seriesly['n1ql_statsperfregression_450-2151-enterprise_d1d'].get_all()   # bad result Q3 stale ok
#s=seriesly['fts_statsfts_450-2151-enterprise_1b7'].get_all()
p={}
c=0
for k in s.keys():
    #if s[k]['query_requests'] == 1:
        #pass #print '\n\n***'
    if field is None:
        print s[k]
    else:
        print s[k][field], ',',
    #print c
    #c += 1
    #p[s[k]['cbft_doc_count']] = k
print
s=OrderedDict(sorted(p.items()))
l=s.keys()

for i in l:
    print s[i],',',i


