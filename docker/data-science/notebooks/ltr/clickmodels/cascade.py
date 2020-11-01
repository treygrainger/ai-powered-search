from ltr.clickmodels.session import build
from collections import Counter, defaultdict

class Model():
    def __init__(self):
        # Attractiveness per query-doc
        self.attracts = defaultdict(lambda : 0.5)

def cascade_model(sessions):
    """ Cascading model can be solved directly:
         - sessions with skips count against a doc
         - sessions with clicks count for
         - stop at first click
        """
    session_counts = Counter()
    click_counts = Counter()
    model=Model()

    for session in sessions:
        for rank, doc in enumerate(session.docs):
            query_doc_key = (session.query, doc.doc_id)
            session_counts[query_doc_key] += 1

            if doc.click:
                # Cascading model doesn't consider
                # clicks past the last one, so we count
                # this one and break out
                click_counts[query_doc_key] += 1
                break;

    for (query_id, doc_id), count in session_counts.items():
        query_doc_key = (query_id, doc_id)
        model.attracts[query_doc_key] = click_counts[query_doc_key] / session_counts[query_doc_key]
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
    cascade_model(sessions)





