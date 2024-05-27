from collections import Counter

def conv_aug_attracts(attracts, sessions, costs):
    """ Rescan sessions, using click-derrived attractiveness.

        If theres no conversion, punish the attractiveness derrived judgment

        BUT we punish costly things less, and cheap things more
    """
    satisfacts = Counter()
    counts = Counter()
    for session in sessions:
        for rank, doc in enumerate(session.docs):
            attract = attracts[(session.query, doc.doc_id)]
            if doc.click:
                if doc.conversion:
                    # Confirms the attractiveness was real with actual relevance
                    counts[(session.query, doc.doc_id)] += 1
                    satisfacts[(session.query, doc.doc_id)] += attract
                else:
                    # If it costs a lot, and there wasn't a conversion,
                    #  thats ok, we default to attractiveness
                    # If it costs little, and there wasn't a conversion,
                    #  thats generally not ok, why didn't they do (easy action)
                    counts[(session.query, doc.doc_id)] += 1
                    satisfacts[(session.query, doc.doc_id)] += attract * costs[doc.doc_id]
            else:
                counts[(session.query, doc.doc_id)] += 1
                satisfacts[(session.query, doc.doc_id)] += attract * costs[doc.doc_id]

    for (query_id, doc_id), count in counts.items():
        satisfacts[(query_id, doc_id)] = satisfacts[(query_id,doc_id)] / count

    return satisfacts


