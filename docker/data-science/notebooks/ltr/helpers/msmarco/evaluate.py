import csv
import gzip


class QRel():

    def __init__(self, qid, docid, keywords):
        self.qid=qid
        self.docid=docid
        self.keywords = keywords

    def eval_rr(self, doc_ranking):
        """ Evaluate the provided doc ranking using reciprical rank
            (1/rank of the expected doc)

            returns 0 if this qrels doc id is missing
        """

        for rank, docid in enumerate(doc_ranking, start=1):
            if docid == self.docid:
                return 1.0 / rank
        return 0.0

    @staticmethod
    def read_qrels(qrels_fname='data/msmarco-doctrain-qrels.tsv.gz',
                   queries_fname='data/msmarco-doctrain-queries.tsv.gz'):

        qids_to_keywords = QRel.get_keyword_lookup(queries_fname)

        with gzip.open(qrels_fname, 'rt') as f:
            reader = csv.reader(f, delimiter=' ')
            for row in reader:
                qid = row[0]
                keywords = None
                if qid in qids_to_keywords:
                    keywords = qids_to_keywords[qid]
                else:
                    print("Missing keywords for %s" % qid)
                yield QRel(qid=row[0], docid=row[2], keywords=keywords)

    @staticmethod
    def get_keyword_lookup(fname='data/msmarco-doctrain-queries.tsv.gz'):
        qids_to_keywords = {}
        with gzip.open(fname, 'rt') as f:
            reader = csv.reader(f, delimiter='\t')
            for row in reader:
                qids_to_keywords[row[0]] = row[1]
        return qids_to_keywords

    def __str__(self):
        return "qid:%s(%s) => doc:%s" % (self.qid, self.keywords, self.docid)


if __name__ == "__main__":
    qrels = {}
    for qrel in QRel.read_qrels():
        qrels[qrel.qid] = qrel

    print(qrels['1185869'].eval_rr(['1','1']))

