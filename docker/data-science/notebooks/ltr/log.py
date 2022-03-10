import re

class FeatureLogger:
    """ Logs LTR Features, one query at a time

        ...Building up a training set...
    """

    def __init__(self, client, index, feature_set, drop_missing=True, id_field='id'):
        self.client=client
        self.index=index
        self.feature_set=feature_set
        self.drop_missing=drop_missing
        self.id_field=id_field
        self.logged=[]

    def clear(self):
        self.logged=[]

    def log_for_qid(self, judgments, qid=None, keywords=None):
        """ Log a set of judgments associated with a single qid
            judgments will be modified, a training set also returned, discarding
            any judgments we could not log features for (because the doc was missing)
        """
        featuresPerDoc = {}
        judgments = [j for j in judgments]
        doc_ids = [judgment.doc_id for judgment in judgments]

        if keywords is None:
            keywords=judgments[0].keywords

        if qid is None:
            qid=judgments[0].qid

        # Check for dups of documents
        for doc_id in doc_ids:
            indices = [i for i, x in enumerate(doc_ids) if x == doc_id]
            if len(indices) > 1:
                print("Duplicate Doc in qid:%s %s" % (qid, doc_id))

        # For every batch of N docs to generate judgments for
        BATCH_SIZE = 500
        numLeft = len(doc_ids)
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
            res = self.client.log_query(index=self.index, featureset=self.feature_set, ids=ids,
                                        options=params, id_field=self.id_field)

            # Add feature back to each judgment
            for doc in res:
                doc_id = str(doc[self.id_field])
                features = doc['ltr_features']
                featuresPerDoc[doc_id] = features
            numLeft -= BATCH_SIZE

        # Append features from search engine back to ranklib judgment list
        for judgment in judgments:
            try:
                if judgment.qid != qid:
                    raise RuntimeError("Judgment qid {} inconsistent with logged qid {}".format(
                        judgment.qid, qid))
                if judgment.keywords != keywords:
                    raise RuntimeError("Judgment keywords {} inconsistent with logged keywords {}".format(
                        judgment.keywords, keywords))
                features = featuresPerDoc[judgment.doc_id] # If KeyError, then we have a judgment but no movie in index
                judgment.features = features
            except KeyError:
                pass
                print("Missing doc %s" % judgment.doc_id)

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
