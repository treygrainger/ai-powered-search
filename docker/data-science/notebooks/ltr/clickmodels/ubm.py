from ltr.clickmodels.session import build
from collections import Counter, defaultdict

class Model():
    def __init__(self):
        # Examine prob per-rank
        # Rank 0 is first displayed on page
        # Rank -1 i
        self.ranks = defaultdict(lambda: 0.4)

        # Attractiveness per query-doc
        self.attracts = defaultdict(lambda : 0.5)


def update_attractiveness(sessions, model):
    """ Run through the step of updating attractiveness
        based on session information and the current rank
        examine probabilities

        Algorithm based on Expectation Maximization derived in
        chapter 4 of "Click Models for Web Search" by
        Chulkin, Markov, de Rijke

    """
    attractions = Counter() #Track query-doc attractiveness in this round
    num_sessions = Counter() #Track num sessions where query-doc appears
    for session in sessions:
        last_click = -1
        for rank, doc in enumerate(session.docs):
            query_doc_key = (session.query, doc.doc_id)
            att = 0
            if doc.click:

                last_click = rank

                att = 1
            else:
                exam = model.ranks[(last_click,rank)]
                assert exam <= 1.0
                doc_a = model.attracts[query_doc_key]
                # Not examined, but attractive /
                # 1 - (examined and attractive)
                # When not clicked:
                #  If somehow this is currently a rank examined
                #  a lot and this doc is historically attractive, then
                #  we might still count it as mostly attractive
                # OR if the doc IS examined a lot AND its not
                #  attractive, then we do the opposite, add
                #  close to 0
                att = (((1 - exam) * doc_a) / (1 - (exam * doc_a)))

            # Store away a_sum and
            assert att <= 1.0
            attractions[query_doc_key] += att
            num_sessions[query_doc_key] += 1
            assert attractions[query_doc_key] <= num_sessions[query_doc_key]

    # Update the main query attractiveness from the attractions / num sessions
    for (query_id, doc_id), a_sum in attractions.items():
        query_doc_key = (query_id, doc_id)
        att = a_sum / num_sessions[query_doc_key]
        assert att <= 1.0
        model.attracts[query_doc_key] = att


def update_examines(sessions, model):
    """ Run through the step of updating position examine
        probabilities given current query-doc attractiveness

        Algorithm based on Expectation Maximization derived in
        chapter 4 of "Click Models for Web Search" by
        Chulkin, Markov, de Rijke

    """
    new_rank_probs = defaultdict(lambda: 0)
    counts = defaultdict(lambda: 0)

    for session in sessions:
        last_click = -1
        for rank, doc in enumerate(session.docs):
            if doc.click:
                new_rank_probs[(last_click, rank)] += 1
                counts[(last_click, rank)] += 1
                if last_click == -1 and rank == 3:
                    print(counts[(last_click,rank)])

                last_click = rank
            else:
                # attractiveness at this query/doc pair
                a_qd = model.attracts[(session.query, doc.doc_id)]
                numerator = (1 - a_qd) * model.ranks[(last_click, rank)]
                denominator = 1 - (a_qd * model.ranks[(last_click, rank)])
                # When not clicked - was it examined? We have to guess!
                #  - If it has seemed very attractive, we assume it
                #    was not examined. Because who could pass up such
                #    a yummy looking search result? (numerator)
                #
                #  - If its not attractive, but this rank gets examined
                #    a lot, the new rank prob is closer to 1
                #    (approaches ranks[rank] / ranks[rank])
                #
                #  - If its not examined much, wont contribute much
                new_rank_probs[(last_click, rank)] += numerator / denominator
                counts[(last_click, rank)] += 1
                if last_click == -1 and rank == 3:
                    print(counts[(last_click,rank)])

    for (last_click, click), count in counts.items():
        model.ranks[(last_click, click)] = new_rank_probs[(last_click, click)] / count


def user_browse_model(sessions, rounds=20):
    """
        Algorithm based on Expectation Maximization derived in
        chapter 4 (table 4.1) of "Click Models for Web Search" by
        Chulkin, Markov, de Rijke

    """
    model=Model()
    for i in range(0,rounds):
        update_attractiveness(sessions, model)
        update_examines(sessions, model)
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
    user_browse_model(sessions, rounds=100)
