import sys
sys.path.append('..')
from aips import *
import requests

def tag_query(post_body):
    return requests.post(SOLR_URL + '/entities/tag?json.nl=map&sort=popularity%20desc&matchText=true&echoParams=all&fl=id,type,canonical_form,name,country:countrycode_s,admin_area:admin_code_1_s,popularity,*_p,command_function', post_body).text