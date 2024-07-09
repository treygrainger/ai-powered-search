def get_classic_rating(year):
    if year > 2010:
        return 0
    elif year > 1990:
        return 1
    elif year > 1970:
        return 2
    elif year > 1950:
        return 3
    else:
        return 4

def get_latest_rating(year):
    if year > 2010:
        return 4
    elif year > 1990:
        return 3
    elif year > 1970:
        return 2
    elif year > 1950:
        return 1
    else:
        return 0

def synthesize(client, featureSet='release', latestTrainingSetOut='data/latest-training.txt', classicTrainingSetOut='data/classic-training.txt'):
    from ltr.judgments import judgments_to_file, Judgment
    print('Generating ratings for classic and latest model')
    NO_ZERO = False

    resp = client.log_query('tmdb', 'release', None)

    docs = []
    for hit in resp:
        feature = list(hit['[features]'].values())[0]
        docs.append([feature]) # Treat features as ordered lists

    # Classic film fan
    judgments = []
    for fv in docs:
        rating = get_classic_rating(fv[0])

        if rating == 0 and NO_ZERO:
            continue

        judgments.append(Judgment(qid=1,doc_id=rating,grade=rating,features=fv,keywords=''))

    with open(classicTrainingSetOut, 'w') as out:
        judgments_to_file(out, judgments)

    judgments = []
    for fv in docs:
        rating = get_latest_rating(fv[0])

        if rating == 0 and NO_ZERO:
            continue

        judgments.append(Judgment(qid=1,doc_id=rating,grade=rating,features=fv,keywords=''))


    with open(latestTrainingSetOut, 'w') as out:
        judgments_to_file(out, judgments)

    print('Done')