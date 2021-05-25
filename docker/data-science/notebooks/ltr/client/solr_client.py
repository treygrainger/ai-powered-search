import os
import requests

from .base_client import BaseClient
from ltr.helpers.convert import convert
from ltr.helpers.handle_resp import resp_msg

class SolrClient(BaseClient):
    def __init__(self, host=None):
        self.solr = requests.Session()
        if host:
            self.solr_base_ep=host
            self.host = 'localhost'
        else:
            self.docker = os.environ.get('LTR_DOCKER') != None

            if self.docker:
                self.host = 'solr'
                self.solr_base_ep = 'http://solr:8983/solr'
            else:
                self.host = 'localhost'
                self.solr_base_ep = 'http://localhost:8983/solr'

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

        resp = requests.get('{}/admin/cores?'.format(self.solr_base_ep), params=params)
        resp_msg(msg="Deleted index {}".format(index), resp=resp, throw=False)

    def create_index(self, index):
        # Presumes there is a link between the docker container and the 'index'
        # directory under docker/solr/ (ie docker/solr/tmdb/ is linked into
        # Docker container configsets)
        params = {
            'action': 'CREATE',
            'name': index,
            'configSet': index,
        }
        resp = requests.get('{}/admin/cores?'.format(self.solr_base_ep), params=params)
        resp_msg(msg="Created index {}".format(index), resp=resp)

    def index_documents(self, index, doc_src, batch_size=350, workers=4):
        import concurrent

        def commit():
            resp = self.solr.get('{}/{}/update?commit=true'.format(self.solr_base_ep, index))
            resp_msg(msg="Committed index {}".format(index), resp=resp)

        def flush(docs):
            resp = self.solr.post('{}/{}/update'.format(
                self.solr_base_ep, index), json=docs)
            resp_msg(msg="{} Docs Sent".format(len(docs)), resp=resp)

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
            resp = requests.delete('{}/{}/schema/model-store/{}'.format(self.solr_base_ep, index, model))
            resp_msg(msg='Deleted {} model'.format(model), resp=resp)

        stores = self.get_feature_stores(index)
        for store in stores:
            resp = requests.delete('{}/{}/schema/feature-store/{}'.format(self.solr_base_ep, index, store))
            resp_msg(msg='Deleted {} Featurestore'.format(store), resp=resp)


    def validate_featureset(self, name, config):
        for feature in config:
            if 'store' not in feature or feature['store'] != name:
                raise ValueError("Feature {} needs to be created with \"store\": \"{}\" ".format(feature['name'], name))

    def create_featureset(self, index, name, ftr_config):
        self.validate_featureset(name, ftr_config)
        resp = requests.put('{}/{}/schema/feature-store'.format(
            self.solr_base_ep, index, name), json=ftr_config)
        resp_msg(msg='Created {} feature store under {}:'.format(name, index), resp=resp)


    def log_query(self, index, featureset, ids, options={}, id_field='id'):
        efi_options = []
        for key, val in options.items():
            efi_options.append('efi.{}="{}"'.format(key, val))

        efi_str = ' '.join(efi_options)

        if ids == None:
            query = "*:*"
        else:
            query = "{{!terms f={}}}{}".format(id_field, ','.join(ids))
            print(query)

        params = {
            'fl': '{},[features store={} {}]'.format(id_field, featureset, efi_str),
            'q': query,
            'rows': 1000,
            'wt': 'json'
        }
        resp = requests.post('{}/{}/select'.format(self.solr_base_ep, index), data=params)
        resp_msg(msg='Searching {}'.format(index), resp=resp)
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
        url = '{}/{}/schema/model-store'.format(self.solr_base_ep, index)
        resp = requests.delete('{}/{}'.format(url, model_name))
        resp_msg(msg='Deleted Model {}'.format(model_name), resp=resp)

        resp = requests.put(url, json=solr_model)
        resp_msg(msg='Created Model {}'.format(model_name), resp=resp)

    def submit_ranklib_model(self, featureset, index, model_name, model_payload):
        """ Submits a Ranklib model, converting it to Solr representation """
        resp = requests.get('{}/{}/schema/feature-store/{}'.format(self.solr_base_ep, index, featureset))
        resp_msg(msg='Submit Model {} Ftr Set {}'.format(model_name, featureset), resp=resp)
        metadata = resp.json()
        features = metadata['features']

        feature_dict = {}
        for idx, value in enumerate(features):
            feature_dict[idx + 1] = value['name']

        feature_mapping, _ = self.feature_set(index, featureset)

        solr_model = convert(model_payload, model_name, featureset, feature_mapping)
        self.submit_model(featureset, index, model_name, solr_model)

    def model_query(self, index, model, model_params, query):
        url = '{}/{}/select?'.format(self.solr_base_ep, index)
        params = {
            'q': query,
            'rq': '{{!ltr model={}}}'.format(model),
            'rows': 10000
        }

        resp = requests.post(url, data=params)
        resp_msg(msg='Search keywords - {}'.format(query), resp=resp)
        return resp.json()['response']['docs']

    def query(self, index, query):
        url = '{}/{}/select?'.format(self.solr_base_ep, index)

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
        url = '{}/{}/analysis/field?'.format(self.solr_base_ep, index)

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
        url = '{}/{}/tvrh/'.format(self.solr_base_ep, index)
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
        url = '{}/{}/tvrh/'.format(self.solr_base_ep, index)

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
        resp = requests.get('{}/{}/schema/feature-store'.format(self.solr_base_ep,
                                                                index))
        response = resp.json()
        return response['featureStores']

    def get_models(self, index):
        resp = requests.get('{}/{}/schema/model-store'.format(self.solr_base_ep,
                                                              index))
        response = resp.json()
        return [model['name'] for model in response['models']]


    def feature_set(self, index, name):
        resp = requests.get('{}/{}/schema/feature-store/{}'.format(self.solr_base_ep,
                                                                   index,
                                                                   name))
        resp_msg(msg='Feature Set {}...'.format(name), resp=resp)

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

        resp = requests.post('{}/{}/select'.format(self.solr_base_ep, index), data=params).json()
        return resp['response']['docs'][0]




