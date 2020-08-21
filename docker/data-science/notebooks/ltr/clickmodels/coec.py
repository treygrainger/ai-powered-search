from collections import Counter

class Model():
    def __init__(self):
        # COEC statistic
        self.coecs = Counter()

        # CTR for each query-doc pair in this session
        self.ctrs = {}

def coec(ctr_by_rank, sessions):
    """ Clicks over expected clicks is a metric
        used for seeing what items get above or
        below average CTR for their rank. From paper

        > Personalized Click Prediction in Sponsored Search
        by Cheng, Cantu Paz

        A COEC > 1 means above average CTR for it's position
        A COEC < 1 means below average

        -ctr_by_rank is the global CTR at each rank position
        -sessions are an array of search session objects

        returned:
        each query-doc pair in provided sessions COEC

        """
    clicks = Counter()
    weighted_impressions = Counter()

    for session in sessions:
        for rank, doc in enumerate(session.docs):
            weighted_impressions[(session.query, doc.doc_id)] += ctr_by_rank[rank]
            if doc.click:
                clicks[(session.query, doc.doc_id)] += 1

    model = Model()
    for query_id, doc_id in weighted_impressions:
        model.coecs[(query_id,doc_id)] = \
                clicks[(query_id,doc_id)] / weighted_impressions[(query_id,doc_id)]

    return model
