import sys
sys.path.append('..')
from aips import *
import requests, urllib

def keyword_search(text):     
    q = urllib.parse.quote(text)
    #print(q)
    #q=text.replace("+", "%2B") #so it doesn't get interpreted as space
    qf="text_t"
    defType="lucene"
     
    return requests.get(SOLR_URL + "/reviews/select?q=" + q + "&qf=" + qf + "&defType=" + defType).text