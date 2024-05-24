from collections import Counter, defaultdict
from ltr.clickmodels.session import build

class Model():
    def __init__(self):
        # Satisfaction per query-doc
        self.satisfacts = defaultdict(lambda: 0.1)

        # Attractiveness per query-doc
        self.attracts = defaultdict(lambda : 0.1)

reverse_enumerate = lambda l: zip(range(len(l)-1, -1, -1), reversed(l))


def sdbn(sessions):
    """ Simplified Dynamic Bayesian Network is a simpler
        version of the much more complex Dynamic Bayesian Network
        that the authors say comes close to the accuracy of DBN

        Most importantly, it can be solved directly and simply without
        an EM learning process

        Features of sdbn:
        - Attractiveness is any click out of sessions where that document
          appears before the last click of the session
        - Satisfaction occurs when a doc is the last document clicked
          out of all sessions where that document is clicked

        """
    model = Model()
    NO_CLICK = -1
    counts = Counter()
    clicks = Counter()
    last_clicks = Counter()
    for session in sessions:
        last_click = NO_CLICK
        for rank, doc in reverse_enumerate(session.docs):
            if last_click == NO_CLICK and doc.click:
                last_click = rank

            if last_click != NO_CLICK:
                query_doc = (session.query, doc.doc_id)
                counts[query_doc] += 1

                if doc.click:
                    # Cascading model doesn't consider
                    # clicks past the last one, so we count
                    # this one and break out
                    clicks[query_doc] += 1
                    if rank == last_click:
                        last_clicks[query_doc] += 1

    # For all meaningful sessions (where query_doc appear)
    # count attractiveness clicks / num sessions
    # count satisfacts last clicks / sessions with clicks
    for query_doc, count in counts.items():
        model.attracts[query_doc] = clicks[query_doc] / count
        if query_doc in clicks:
            model.satisfacts[query_doc] = last_clicks[query_doc] / clicks[query_doc]
    return model


if __name__ == "__main__":
    sessions = build([
      ('A', ((1, True), (2, False), (3, True), (0, False))),
      ('B', ((5, False), (2, True), (3, True), (0, False))),
      ('A', ((1, False), (2, False), (3, True), (0, False))),
      ('B', ((1, False), (2, False), (3, False), (9, True))),
      ('A', ((9, False), (2, False), (1, True), (0, True))),
      ('B', ((6, True), (2, False), (3, True), (1, False))),
      ('A', ((7, False), (4, True), (1, False), (3, False))),
      ('B', ((8, True), (2, False), (3, True), (1, False))),
      ('A', ((1, False), (4, True), (2, False), (3, False))),
      ('B', ((7, True), (4, False), (5, True), (1, True))),
    ])
    model = sdbn(sessions)
    print(model.attracts[('A', 1)])
    print(model.satisfacts[('A', 1)])
    print(model.attracts[('B', 1)])
    print(model.satisfacts[('B', 1)])
