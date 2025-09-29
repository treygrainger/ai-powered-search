import os
import numpy
from aips.indexer import download_data_files, build_collection
from ltr.log import FeatureLogger
from ltr.judgments import Judgment, judgments_open, judgments_to_file
from itertools import groupby            
from math import sqrt

def normalize_features(logged_judgments):
    all_features = []
    means = [0,0,0]
    for judgment in logged_judgments:
        for idx, f in enumerate(judgment.features):
            means[idx] += f
        all_features.append(judgment.features)
    
    for i in range(len(means)):
        means[i] /= len(logged_judgments)
    
    std_devs = [0.0, 0.0, 0.0]
    for judgment in logged_judgments:
        for idx, f in enumerate(judgment.features):
            std_devs[idx] += (f - means[idx])**2
    for i in range(len(std_devs)):
        std_devs[i] /= len(logged_judgments)
        std_devs[i] = sqrt(std_devs[i])
        
    normed_judgments = []
    for judgment in logged_judgments:
        normed_features = [0.0] * len(judgment.features)
        for idx, f in enumerate(judgment.features):
            normed = (f - means[idx]) / std_devs[idx]
            normed_features[idx] = normed
        normed_judgment = Judgment(qid=judgment.qid,
                                   keywords=judgment.keywords,
                                   doc_id=judgment.doc_id,
                                   grade=judgment.grade,
                                   features=normed_features)
        normed_judgment.old_features = judgment.features
        normed_judgments.append(normed_judgment)

    return means, std_devs, normed_judgments

def pairwise_transform(normed_judgments):
    predictor_deltas = []
    feature_deltas = []
    for qid, query_judgments in groupby(normed_judgments, key=lambda j: j.qid):
        query_judgments = list(query_judgments)
        for doc1_judgment in query_judgments:
            for doc2_judgment in query_judgments:
                j1_features = numpy.array(doc1_judgment.features)
                j2_features = numpy.array(doc2_judgment.features)                            
                if doc1_judgment.grade > doc2_judgment.grade:
                    predictor_deltas.append(1)
                    feature_deltas.append(j1_features - j2_features)
                elif doc1_judgment.grade < doc2_judgment.grade:
                    predictor_deltas.append(-1)
                    feature_deltas.append(j1_features - j2_features)
    return numpy.array(feature_deltas), numpy.array(predictor_deltas)

def generate(engine, force_rebuild=False):
    if os.path.isfile("data/feature_data.npy") and not force_rebuild:
        return
    
    download_data_files("judgments")
    tmdb_collection = build_collection(engine, "tmdb")
    ftr_logger=FeatureLogger(index=tmdb_collection, feature_set="movie_model")

    with judgments_open("data/ai_pow_search_judgments.txt") as judgment_list:
        for qid, query_judgments in groupby(judgment_list, key=lambda j: j.qid):
            ftr_logger.log_for_qid(judgments=query_judgments, qid=qid,
                                   keywords=judgment_list.keywords(qid))

    means, std_devs, normed_judgments = normalize_features(ftr_logger.logged)
    feature_deltas, predictor_deltas = pairwise_transform(normed_judgments)

    with open("data/feature_data.npy", "wb") as f:
        num_features = 3
        feature_data = numpy.append(feature_deltas, [means, std_devs])
        rows = feature_data.shape[0]//num_features    
        cols = num_features
        feature_data = feature_data.reshape((rows, cols))
        
        numpy.save(f, feature_data)
        
    with open("data/predictor_deltas.npy", "wb") as f:
        numpy.save(f, predictor_deltas )
        
    with open("data/normed_judgments.txt", "wt") as f:
        judgments_to_file(f, normed_judgments)