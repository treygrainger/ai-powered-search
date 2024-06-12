import re
from aips import get_ltr_engine, get_engine

class FeatureLogger:
    """ Logs LTR Features, one query at a time

        ...Building up a training set...
    """

    def __init__(self, engine, index, feature_set, drop_missing=True, id_field='id'):
        self.engine=engine
        self.index=index
        self.feature_set=feature_set
        self.drop_missing=drop_missing
        self.id_field=id_field
        self.logged=[]

    def clear(self):
        self.logged=[]

    def log_for_qid(self, judgments, qid=None, keywords=None, log=False):
        """ Log a set of judgments associated with a single qid
            judgments will be modified, a training set also returned, discarding
            any judgments we could not log features for (because the doc was missing)
        """
        if qid is None:
            qid=judgments[0].qid

        judgments = [j for j in judgments]
        doc_ids = [judgment.doc_id for judgment in judgments]
        unique_ids = list(set(doc_ids))
        if len(doc_ids) != len(unique_ids):
            duplicated = set([id for id in doc_ids if doc_ids.count(id) > 1])
            print(f"Duplicate docs in for query id {qid}: {duplicated}")
        doc_ids = unique_ids

        if keywords is None:
            keywords = judgments[len(judgments) - 1].keywords
        # For every batch of N docs to generate judgments for
        BATCH_SIZE = 500
        numLeft = len(doc_ids)
        document_features = {}
        for i in range(0, 1 + (len(doc_ids) // BATCH_SIZE)):

            numFetch = min(BATCH_SIZE, numLeft)
            start = i*BATCH_SIZE
            if start >= len(doc_ids):
                break
            ids = doc_ids[start:start+numFetch]

            # Sanitize (Solr has a strict syntax that can easily be tripped up)
            # This removes anything but alphanumeric and spaces
            fixed_keywords = re.sub('([^\s\w]|_)+', '', keywords)

            params = {
                "keywords": fixed_keywords,
                "fuzzy_keywords": ' '.join([x + '~' for x in fixed_keywords.split(' ')]),
                "squeezed_keywords": ''.join(fixed_keywords.split(' '))
            }

            ids = [str(doc_id) for doc_id in ids]
            res = get_ltr_engine(self.index).get_logged_features(self.feature_set, ids,
                                             params, id_field=self.id_field, log=log)


            # Add feature back to each judgment
            for doc in res:
                doc_id = str(doc[self.id_field])
                features = doc['[features]']
                document_features[doc_id] = list(features.values())
            numLeft -= BATCH_SIZE

        # Append features from search engine back to ranklib judgment list
        for judgment in judgments:
            if judgment.qid != qid:
                raise RuntimeError(f"Judgment qid {judgment.qid} inconsistent with logged qid {qid}")
            if judgment.keywords != keywords:
                raise RuntimeError(f"Judgment keywords {judgment.keywords} inconsistent with logged keywords {keywords}")
            if judgment.doc_id not in document_features:                
                print(f"Missing doc {judgment.doc_id} with error")
                continue
            judgment.features = document_features[judgment.doc_id]

        # Return a paired down judgments if we are missing features for judgments
        training_set = []
        discarded = []
        for judgment in judgments:
            if self.drop_missing:
                if judgment.has_features():
                    training_set.append(judgment)
                else:
                    discarded.append(judgment)
            else:
                training_set.append(judgment)
        # print("Discarded %s Keep %s" % (len(discarded), len(training_set)))
        self.logged.extend(training_set)
        return training_set, discarded
