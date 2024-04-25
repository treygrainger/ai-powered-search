import pandas
import numpy
import random

def popular_query_events(signals, min_query_count):
    """ All query events for queries occuring > min_query_count times """
    queries = signals[signals['type'] == 'query']
    popular_queries = queries.groupby('target').count() \
                             .rename(columns={'query_id': 'query_count'}) \
                             .sort_values('query_count', ascending=False)
    popular_queries = popular_queries[popular_queries['query_count'] > min_query_count].index.to_list()
    pop_query_events = signals[signals['type'] == 'query'][signals['target'].isin(popular_queries)]
    query_events = pop_query_events[['query_id', 'target']].rename(columns={'target': 'query'})
    return query_events

def clicks_joined_with_queries(signals, query_events):
    clicks = signals[signals['type'] == 'click']
    click_events = clicks[['query_id', 'target']].rename(columns={'target': 'clicked_doc_id'})
    queries_with_clicked_docs = query_events.merge(click_events,
                                                   on='query_id',
                                                   how='left')
    queries_with_clicked_docs['clicked_doc_id'] = queries_with_clicked_docs['clicked_doc_id'].fillna(0)
    return queries_with_clicked_docs

def compute_ctr(queries_with_clicked_docs, query_counts):
    # click counts per doc in query
    click_thru_rate = queries_with_clicked_docs.groupby(['query', 'clicked_doc_id'])\
            .count()\
            .rename(columns={'query_id':'click_count'}).reset_index()
    click_thru_rate = click_thru_rate.merge(query_counts, on='query', how='left')

    click_thru_rate['ctr'] = click_thru_rate['click_count'] / click_thru_rate['tot_query_count']
    click_thru_rate = click_thru_rate.sort_values(['query', 'ctr'], ascending=[True, False])
    return click_thru_rate


def build_ground_truth(signals_path, min_query_count):
    """ Build a canonical, ground truth from the signals
        (assumes for retrotech
         CTR is actually reflecting the source systems usual ranking, and
         we can use this plus CTR stats to shuffle docs around and simulate
         a less accurate search system... then use click  models, etc to
         get closer to that ground truth
    """
    signals = pandas.read_csv(signals_path)

    # Unique query events, like
    # u_1_1, bluray
    # u_1_2, dvd player
    # u_1_3, bluray
    query_events = popular_query_events(signals, min_query_count)

    # Each query event along with associated clicked docs
    # during that session
    # u_1_1, bluray, 1234567
    # u_1_2, dvd player, 12521125
    # u_1_3, bluray, 0
    queries_with_clicked_docs = clicks_joined_with_queries(signals, query_events)

    # CTR for each doc per query like:
    # bluray, 1234567, 0.121
    query_counts = query_events.groupby('query').count().rename(columns={'query_id': 'tot_query_count'})
    click_thru_rate = compute_ctr(queries_with_clicked_docs, query_counts)

    # Get rid of doc_id 0, which is all the queries with no clicks
    # We treat these as a canonical ranking from the source system, assume it's relatively
    # highly tuned and the source CTRs are pretty reasonably close to actual relevance ranking
    # in the source system. Of course this is a dubious assumption in a real search system,
    # but for our purposes - to synthesize reasonable looking search sessions - it will serve
    canonical_rankings = click_thru_rate[click_thru_rate['clicked_doc_id'] != 0].reset_index()

    canonical_rankings['rank'] = canonical_rankings.groupby('query').cumcount()

    # Compute how good a given doc is in its rank (does in underperform CTR < median or mean; or
    # overperform CTR > median or mean). This is recorded as a z-score
    # If we shufflle this doc up or down in a ranking, we can use the mean CTR for that rank
    # and the z-score as a means to compute the expected CTR for that rank
    # (of course, this is all for the sake of creating simulated sessions, not seeking exact
    #  science. Esp since CTRs aren't normally distributed, so z-scores will be rough measures...)
    max_depth = canonical_rankings['rank'].max()
    for i in range(0, max_depth):
        idxs = canonical_rankings[canonical_rankings['rank'] == i].index

        # Mean based statistics
        canonical_rankings.loc[idxs, 'posn_ctr_mean'] = \
                canonical_rankings[canonical_rankings['rank'] == i]['ctr'].mean()
        canonical_rankings.loc[idxs, 'posn_ctr_std'] = \
                canonical_rankings[canonical_rankings['rank'] == i]['ctr'].std()

        # Median based statistics (more outlier prone)
        canonical_rankings.loc[idxs, 'posn_ctr_median'] = \
                canonical_rankings[canonical_rankings['rank'] == i]['ctr'].median()
        canonical_rankings.loc[idxs, 'posn_ctr_mad'] = \
                canonical_rankings[canonical_rankings['rank'] == i]['ctr'].mad()

    canonical_rankings['ctr_std_z_score'] = \
            ((canonical_rankings['ctr'] - canonical_rankings['posn_ctr_mean'])
             / canonical_rankings['posn_ctr_std'])
    canonical_rankings['ctr_mod_z_score'] = \
            ((canonical_rankings['ctr'] - canonical_rankings['posn_ctr_median'])
              / canonical_rankings['posn_ctr_mad'])

    return canonical_rankings


class SessionGenerator:
    def __init__(self, signals_path='../data/retrotech/signals.csv', min_query_count=100):
        self.canonical_rankings = build_ground_truth(signals_path, min_query_count)
        self.curr_sess_id = 1
        self.random_rankings = {}

    def _do_swap(self, top_n, max_rank_to_swap, pairs_to_swap):
        to_swap = list(range(0,max_rank_to_swap))
        random.shuffle(to_swap)
        to_swap = to_swap[:(pairs_to_swap*2)]
        for a, b in zip(to_swap, to_swap[1:]):
            b_val = top_n.iloc[b]
            a_val = top_n.iloc[a].copy()
            top_n.iloc[a] = b_val
            top_n.iloc[b] = a_val
        return top_n

    def _random_ranking(self, max_rank, query):
        """ from ground truth -> random ranking that will seed all session
            generation (with slight swaps each session) """
        if query not in self.random_rankings:
            shuffled = self.canonical_rankings[self.canonical_rankings['query'] == query]
            shuffled = shuffled[['posn_ctr_mean', 'posn_ctr_std', 'rank', 'posn_ctr_mad', 'posn_ctr_median']]\
                        .rename(columns={'rank': 'dest_rank'})
            shuffled = self._do_swap(top_n=shuffled[shuffled['dest_rank'] < max_rank],
                                     max_rank_to_swap=max_rank//2, pairs_to_swap=4)
            shuffled = self._do_swap(top_n=shuffled[shuffled['dest_rank'] < max_rank],
                                     max_rank_to_swap=max_rank, pairs_to_swap=2)
            self.random_rankings[query] = shuffled
        return self.random_rankings[query]

    def _shuffled_ranking(self, max_rank, query, pairs_to_swap=4):
        """ Seeded with a random shuffling of the ground truth, swap
            <pairs_to_swap> docs in the ranking and return this as
            the listing for this session """
        top_n = self._random_ranking(max_rank, query).copy()
        top_n = self._do_swap(top_n=top_n, max_rank_to_swap=max_rank, pairs_to_swap=pairs_to_swap)
        # Swap a few posns

        return top_n


    def __call__(self, query, num_docs=30, use_median=False, dampen=1.0):
        """ Build a single session for query given sess_id"""
        canonical = self.canonical_rankings[self.canonical_rankings['query'] == query][self.canonical_rankings['rank'] < num_docs]
        if len(canonical) == 0:
            raise KeyError(f"Query: {query} not present in signal data")

        top_n = self._shuffled_ranking(num_docs, query)

        shuffled = top_n.rename(columns={'posn_ctr_std': 'dest_ctr_std',
                             'posn_ctr_mean': 'dest_ctr_mean',
                             'posn_ctr_mad': 'dest_ctr_mad',
                             'posn_ctr_median': 'dest_ctr_median'})
        shuffled = shuffled.reset_index(drop=True).reset_index().rename(columns={'index': 'rank'})
        shuffled = shuffled.merge(canonical, on='rank', how='left')
        shuffled['dest_ctr_median_based'] = (
                (shuffled['ctr_mod_z_score'] * dampen * shuffled['dest_ctr_mad'])
                + (shuffled['dest_ctr_median'])
        )
        shuffled['dest_ctr_mean_based'] = (
                (shuffled['ctr_std_z_score'] * dampen * shuffled['dest_ctr_std'])
                + (shuffled['dest_ctr_mean'])
        )
        shuffled['draw'] = numpy.random.rand(len(shuffled))
        if not use_median:
            shuffled['clicked'] = shuffled['draw'] < shuffled['dest_ctr_median_based']
        else:
            shuffled['clicked'] = shuffled['draw'] < shuffled['dest_ctr_mean_based']
        shuffled['sess_id'] = self.curr_sess_id

        self.curr_sess_id += 1
        return shuffled
