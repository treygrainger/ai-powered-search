import os
import requests
import re

from .base_client import BaseClient
from ltr.helpers.convert import convert
from ltr.helpers.handle_resp import resp_msg

class SolrClient(BaseClient):
    def __init__(self, solr_base=None):
        self.solr = requests.Session()
        if solr_base:
            self.solr_base_ep=solr_base
            if solr_base.endswith('/'):
                self.solr_base_ep=solr_base[:len(solr_base)-1]
            self.host = re.sub(r"https?://([^/:]+)((:[0-9]+)?/.*)?$", "\\1", solr_base)
        else:
            self.docker = os.environ.get('LTR_DOCKER') != None

            if self.docker:
                self.host = 'solr'
            else:
                self.host = 'localhost'

            self.solr_base_ep = f'http://{self.host}:8983/solr'

    def get_host(self):
        return self.host

    def name(self):
        return "solr"

    def delete_index(self, index):
        params = {
            'action': 'UNLOAD',
            'core': index,
            'deleteIndex': 'true',
            'deleteDataDir': 'true',
            'deleteInstanceDir': 'true'
        }

        resp = requests.get(f'{self.solr_base_ep}/admin/cores?', params=params)
        resp_msg(msg=f"Deleted index {index}", resp=resp, throw=False)

    def create_index(self, index):
        # Presumes there is a link between the docker container and the 'index'
        # directory under docker/solr/ (ie docker/solr/tmdb/ is linked into
        # Docker container configsets)
        params = {
            'action': 'CREATE',
            'name': index,
            'configSet': index,
        }
        resp = requests.get(f'{self.solr_base_ep}/admin/cores?', params=params)
        resp_msg(msg=f"Created index {index}", resp=resp)

    def index_documents(self, index, doc_src, batch_size=350, workers=4):
        import concurrent

        def commit():
            resp = self.solr.get(f'{self.solr_base_ep}/{index}/update?commit=true')
            resp_msg(msg=f"Committed index {index}", resp=resp)

        def flush(docs):
            resp = self.solr.post(f'{self.solr_base_ep}/{index}/update', json=docs)
            resp_msg(msg=f"{len(docs)} Docs Sent", resp=resp)

        docs = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for doc in doc_src:
                if 'release_date' in doc and doc['release_date'] is not None:
                    doc['release_date'] += 'T00:00:00Z'

                docs.append(doc)

                if len(docs) % batch_size == 0:
                    executor.submit(flush, docs)
                    docs = []

            executor.submit(flush, docs)
        commit()

    def reset_ltr(self, index):
        models = self.get_models(index)
        for model in models:
            resp = requests.delete(f'{self.solr_base_ep}/{index}/schema/model-store/{model}')
            resp_msg(msg=f'Deleted {model} model', resp=resp)

        stores = self.get_feature_stores(index)
        for store in stores:
            resp = requests.delete(f'{self.solr_base_ep}/{index}/schema/feature-store/{store}')
            resp_msg(msg=f'Deleted {store} Featurestore', resp=resp)


    def validate_featureset(self, name, config):
        for feature in config:
            if 'store' not in feature or feature['store'] != name:
                raise ValueError(f"Feature {feature['name']} needs to be created with \"store\": \"{name}\" ")

    def create_featureset(self, index, name, ftr_config):
        self.validate_featureset(name, ftr_config)
        resp = requests.put(f'{self.solr_base_ep}/{index}/schema/feature-store/{name}', json=ftr_config)
        resp_msg(msg=f'Created {name} feature store under {index}:', resp=resp)


    def log_query(self, index, featureset, ids, options={}, id_field='id', log=True):
        efi_options = []
        for key, val in options.items():
            efi_options.append(f'efi.{key}="{val}"')

        efi_str = ' '.join(efi_options)

        if ids == None:
            query = "*:*"
        else:
            query = "{{!terms f={}}}{}".format(id_field, ','.join(ids))
            if log:
                print(query)

        params = {
            'fl': f"{id_field},[features store={featureset} {efi_str}]",
            'q': query,
            'rows': 1000,
            'wt': 'json'
        }
        
        resp = requests.post(f'{self.solr_base_ep}/{index}/select', data=params)
        if log:
            resp_msg(msg=f'Searching {index}', resp=resp)
        resp = resp.json()

        def parseFeatures(features):
            fv = []

            all_features = features.split(',')

            for feature in all_features:
                elements = feature.split('=')
                fv.append(float(elements[1]))

            return fv

        # Clean up features to consistent format
        for doc in resp['response']['docs']:
            doc['ltr_features'] = parseFeatures(doc['[features]'])

        return resp['response']['docs']

    def submit_model(self, featureset, index, model_name, solr_model):
        url = f'{self.solr_base_ep}/{index}/schema/model-store'
        resp = requests.delete(f'{url}/{model_name}')
        resp_msg(msg=f'Deleted Model {model_name}', resp=resp)

        resp = requests.put(url, json=solr_model)
        resp_msg(msg=f'Created Model {model_name}', resp=resp)

    def submit_ranklib_model(self, featureset, index, model_name, model_payload):
        """ Submits a Ranklib model, converting it to Solr representation """
        resp = requests.get(f'{self.solr_base_ep}/{index}/schema/feature-store/{featureset}')
        resp_msg(msg=f'Submit Model {model_name} Ftr Set {featureset}', resp=resp)
        metadata = resp.json()
        features = metadata['features']

        feature_dict = {}
        for idx, value in enumerate(features):
            feature_dict[idx + 1] = value['name']

        feature_mapping, _ = self.feature_set(index, featureset)

        solr_model = convert(model_payload, model_name, featureset, feature_mapping)
        self.submit_model(featureset, index, model_name, solr_model)

    def model_query(self, index, model, model_params, query):
        url = f'{self.solr_base_ep}/{index}/select?'
        params = {
            'q': query,
            'rq': '{{!ltr model={}}}'.format(model),
            'rows': 10000
        }

        resp = requests.post(url, data=params)
        resp_msg(msg=f'Search keywords - {query}', resp=resp)
        return resp.json()['response']['docs']

    def query(self, index, query):
        url = f'{self.solr_base_ep}/{index}/select?'

        resp = requests.post(url, data=query)
        #resp_msg(msg='Query {}...'.format(str(query)[:20]), resp=resp)
        resp = resp.json()

        # Transform to be consistent
        for doc in resp['response']['docs']:
            if 'score' in doc:
                doc['_score'] = doc['score']

        return resp['response']['docs']

    def analyze(self, index, fieldtype, text):
        # http://localhost:8983/solr/msmarco/analysis/field
        url = f'{self.solr_base_ep}/{index}/analysis/field?'

        query={
            "analysis.fieldtype": fieldtype,
            "analysis.fieldvalue": text
        }

        resp = requests.post(url, data=query)

        analysis_resp = resp.json()
        tok_stream = analysis_resp['analysis']['field_types']['text_general']['index']
        tok_stream_result = tok_stream[-1]
        return tok_stream_result

    def term_vectors_skip_to(self, index, q='*:*', skip=0):
        url = f'{self.solr_base_ep}/{index}/tvrh/'
        query={
            'q': q,
            'cursorMark': '*',
            'sort': 'id asc',
            'fl': 'id',
            'rows': str(skip)
            }
        tvrh_resp = requests.post(url, data=query)
        return tvrh_resp.json()['nextCursorMark']

    def term_vectors(self, index, field, q='*:*', start_cursor='*'):
        """ Extract all term vectors for a field
        """
        # http://localhost:8983/solr/msmarco/tvrh?q=*:*&start=0&rows=10&fl=id,body&tvComponent=true&tv.positions=true
        url = f'{self.solr_base_ep}/{index}/tvrh/'

        def get_posns(weird_posns):
            positions = []
            if weird_posns[0] == 'positions':
                for posn in weird_posns[1]:
                    if posn == "position":
                        continue
                    else:
                        positions.append(posn)


        next_cursor = start_cursor
        while True:

            query={
                "q": q,
                "cursorMark": next_cursor,
                "fl": "id",
                "tv.fl": field,
                "tvComponent": 'true',
                "tv.positions": 'true',
                'sort': 'id asc',
                'rows': '2000',
                'wt': 'json'
            }

            tvrh_resp = requests.post(url, data=query).json()

            from ltr.client.solr_parse import parse_termvect_namedlist
            parsed = parse_termvect_namedlist(tvrh_resp['termVectors'], field=field)
            for doc_id, terms in parsed.items():
                try:
                    yield doc_id, terms[field]
                except KeyError:
                    yield doc_id, {}

            next_cursor = tvrh_resp['nextCursorMark']

            if query['cursorMark'] == next_cursor:
                break



    def get_feature_stores(self, index):
        resp = requests.get(f'{self.solr_base_ep}/{index}/schema/feature-store')
        response = resp.json()
        return response['featureStores']

    def get_models(self, index):
        resp = requests.get(f'{self.solr_base_ep}/{index}/schema/model-store')
        response = resp.json()
        return [model['name'] for model in response['models']]


    def feature_set(self, index, name):
        resp = requests.get(f'{self.solr_base_ep}/{index}/schema/feature-store/{name}')
        resp_msg(msg=f'Feature Set {name}...', resp=resp)

        response = resp.json()

        rawFeatureSet = response['features']

        mapping = []
        for feature in response['features']:
            mapping.append({'name': feature['name']})

        return mapping, rawFeatureSet

    def get_doc(self, index, doc_id, fields=None):
        if fields is None:
            fields = ['*', 'score']
        params = {
            'q': 'id:{}'.format(doc_id),
            'wt': 'json',
            'fl': ','.join(fields)
        }

        resp = requests.post(f'{self.solr_base_ep}/{index}/select', data=params).json()
        return resp['response']['docs'][0]




