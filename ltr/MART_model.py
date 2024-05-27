import xml.etree.ElementTree as ET
from ltr.judgments import _judgments_by_qid

def fold_whoopsies(whoopsies1, whoopsies2):
    """ Merge whoopsies2 into whoopsies1
        sorted on query, then descending on magnitude
        of the whoops (so biggest whoops for queries come first)"""
    whoopsies1.extend(whoopsies2)
    whoopsies1.sort(key=lambda x: (x.qid, 1000-x.magnitude()))
    return whoopsies1

def dedup_whoopsies(sortedWhoopsies):
    """ Take whoopsies sorted first by qid, then
        magnitude, then return the worst whoopsie
        by query """
    mergedWhoopsies = iter(sortedWhoopsies)

    whoopsies = []
    whoopsie = None
    lastQid = -1
    try:
        while True:
            # Read ahead to next query
            while whoopsie is None or lastQid == whoopsie.qid:
                whoopsie = next(mergedWhoopsies)
            whoopsies.append(whoopsie)
            lastQid = whoopsie.qid
    except StopIteration:
        pass
    return whoopsies


class MARTModel:
    def __init__(self, ranklib_xml, features):
        """ Create a MART model from a ranklib_ensemble
            (string w/ ranklib xml model output)
            using `features` - array of named features
            where the 0th item is ranklib feature 1
             [{'name': 'release_date'}, ...] """
        # Clean up header
        valid = False
        linesSplit = ranklib_xml.split('\n')
        if linesSplit[0] == "## LambdaMART":
            print("Whoopsies on LAMBDAMart")
            valid = True
        if linesSplit[0] == "## Random Forests":
            if linesSplit[1] == "## No. of bags = 1":
                print("RF with 1 bag")
                valid = True

        if (not valid):
            raise ValueError("Whoopsies only support LambdaMART of Random Forest of bags=1")

        headerAt = 0
        for line in linesSplit:
            if len(line) > 0 and line[0] == '#':
                headerAt += 1
            else:
                break;

        print("Header At %s" % headerAt)
        validXml = '\n'.join(ranklib_xml.split('\n')[headerAt:])
        lambdaModel = ET.fromstring(validXml)

        # List of tuples (weight, root split)
        self.trees = []
        for node in lambdaModel:
            self.trees.append((float(node.attrib['weight']),
                              Split(node[0], features)) )

    def __str__(self):
        rVal = ""
        for tree in self.trees:
            weight = tree[0]
            tree = tree[1]
            rVal += tree.treeString(weight=weight)
            rVal += "\n\n"
        return rVal

    def whoopsies(self):
        """ After eval, what are the most glaring
            query-doc inconsistencies in the provided judgments
            over the whole ensemble """
        whoopsQueries = {}
        perTreeWhoops = [None for _ in self.trees]
        for treeNo, tree in enumerate(self.trees):
            treeWhoopsies = tree[1].whoopsies()

            for whoops in dedup_whoopsies(treeWhoopsies):
                if whoops.qid not in whoopsQueries:
                    whoopsQueries[whoops.qid] = QueryWhoopsie(qid=whoops.qid,
                                                              totalMagnitude=0,
                                                              minGrade=0,
                                                              count=0,
                                                              maxGrade=0,
                                                              perTreeWhoops=perTreeWhoops)

                whoopsQueries[whoops.qid].count += 1
                whoopsQueries[whoops.qid].totalMagnitude += whoops.magnitude()
                whoopsQueries[whoops.qid].minGrade = whoops.minGrade
                whoopsQueries[whoops.qid].maxGrade = whoops.maxGrade
                whoopsQueries[whoops.qid].perTreeWhoops[treeNo] = whoops

        return whoopsQueries



    def eval(self, judgments):
        for tree in self.trees:
            # weight = tree[0]
            tree = tree[1]
            tree.eval(judgments)


class QueryWhoopsie:
    def __init__(self, qid, totalMagnitude,
                 count, maxGrade, minGrade,
                 perTreeWhoops):
        self.qid = qid
        self.count = count
        self.totalMagnitude = totalMagnitude
        self.maxGrade=maxGrade
        self.minGrade=minGrade
        self.perTreeWhoops = perTreeWhoops

    def perTreeReport(self):
        treeSummary = []
        for treeNo, whoops in enumerate(self.perTreeWhoops):
            if whoops is None:
                treeSummary.append("<None>")
            else:
                treeSummary.append("tree:%s=>%s(%s)-%s(%s)" % (treeNo, whoops.minGrade, whoops.minGradeDocId, whoops.maxGrade, whoops.maxGradeDocId))
        return ";".join(treeSummary)


class Whoopsie:

    def __init__(self, qid, judgList,
                 minGrade, maxGrade,
                 minGradeDocId, maxGradeDocId,
                 output):
        self.qid = qid; self.judgList = judgList
        self.minGrade = minGrade; self.maxGrade = maxGrade
        self.minGradeDocId = minGradeDocId; self.maxGradeDocId = maxGradeDocId
        self.output = output

    def magnitude(self):
        return self.maxGrade - self.minGrade


class EvalReport:

    def __init__(self, split):
        if split.output is None:
            raise ValueError("Split not a leaf")

        self.split = split
        self.count = len(split.evals)
        self.whoopsies = []

        self.computeWhoopsies()

    def computeWhoopsies(self):
        judgmentsByQid = _judgments_by_qid(self.split.evals)
        report = []
        for qid, judgList in judgmentsByQid.items():
            if len(judgList) > 1:
                minGradeDocId = judgList[0].doc_id
                maxGradeDocId = judgList[0].doc_id
                minGrade = maxGrade = judgList[0].grade
                for judg in judgList:
                    if judg.grade < minGrade:
                        minGrade = judg.grade
                        minGradeDocId = judg.doc_id
                    if judg.grade > maxGrade:
                        maxGrade = judg.grade
                        maxGradeDocId = judg.doc_id
                if minGrade != maxGrade:
                    report.append(Whoopsie(qid=qid, judgList=judgList,
                                           minGrade=minGrade, maxGrade=maxGrade,
                                           minGradeDocId=minGradeDocId, maxGradeDocId=maxGradeDocId,
                                           output=self.split.output))
        report.sort(key=lambda x: x.maxGrade - x.minGrade, reverse=True)
        self.whoopsies = report

    def __str__(self):
        reportStr = ";".join(["qid:%s:%s(%s)-%s(%s)" % (report.qid, report.minGrade,
                                                        report.minGradeDocId,
                                                        report.maxGrade,
                                                        report.maxGradeDocId)
                            for report in self.whoopsies])
        return "%s/%s/%s" % (self.count, len(self.whoopsies), reportStr)

    def __repr__(self):
        return str(self)



class Split:
    def __init__(self, splitEl, features):
        self.feature = None     # Name of the feature
        self.featureOrd = None  # ONE BASED, feature ord in the ranklib model
        self.featureIdx = None  # Zero BASED - use for lookups
        self.threshold = None
        self.value = None
        self.left = None
        self.right = None
        self.output = None
        self.evalReport = None

        self.evals = []

        for el in splitEl:
            if (el.tag == 'feature'):
                self.featureOrd = int(el.text.strip())
                self.featureIdx = self.featureOrd - 1
                self.feature = features[self.featureIdx]['name']
            elif (el.tag == 'threshold'):
                self.threshold = float(el.text.strip())
            elif (el.tag == 'split' and 'pos' in el.attrib):
                if el.attrib['pos'] == 'right':
                    self.right = Split(splitEl=el, features=features)
                elif el.attrib['pos'] == 'left':
                    self.left = Split(splitEl=el, features=features)
                else:
                    raise ValueError("Unrecognized Split Pos {}".format(el.attrib['pos']))
            elif (el.tag == 'output'):
                self.output = float(el.text.strip())


    def clearEvals(self):
        """ Clear the eval state """
        if self.output:
            self.evals = []
            self.evalReport = None
        elif self.right:
            self.right.clearEvals()
        elif self.left:
            self.left.clearEvals()

    def _evalAppend(self, judgment):
        """ For model/feature analysis purposes, evaluate the model with
            the 'judgment' placing at each leaf the obj
            so we can analyze how well the model is classifying items

            Args:
            - judgment: some Python object with a features attribute
                               which is a list of floating point numbers where
                               0th corresponds to Ranklib's '1'th
            """
        if self.output:
            self.evals.append(judgment)
            return

        ftrToEval = self.featureIdx
        featureVal = judgment.features[ftrToEval]
        if featureVal > self.threshold:
            assert self.right is not None
            self.right._evalAppend(judgment)
        else:
            assert self.left is not None
            self.left._evalAppend(judgment)

    def _computeEvalStats(self):
        if self.output:
            self.evalReport = EvalReport(self)
            return
        else:
            assert self.right is not None
            assert self.left is not None
            self.right._computeEvalStats()
            self.left._computeEvalStats()

    def eval(self, judgments):
        self.clearEvals()
        for judgment in judgments:
            self._evalAppend(judgment)
        self._computeEvalStats()

    def whoopsies(self):
        """ Return all the whoopsies from the child nodes in
            a list of whoopsies ordered first by qid, then by
            magnitude descending. IE (1,4),(1,3),(2,2),(2,0)..."""
        if self.output:
            if self.evalReport is None:
                return []
            return self.evalReport.whoopsies
        else:
            assert self.right is not None
            assert self.left is not None
            rWhoopsies = self.right.whoopsies()
            lWhoopsies = self.left.whoopsies()
            return fold_whoopsies(lWhoopsies, rWhoopsies)


    def treeString(self, weight=1.0, nestLevel=0):

        def idt(nestLevel):
            #return ("%s" % nestLevel) * 2 * nestLevel
            return (" ") * 2 * nestLevel

        rVal = ""
        if self.feature:
            rVal += idt(nestLevel)
            rVal +=  "if %s > %s:\n" % (self.feature, self.threshold)
            assert self.right is not None
            assert self.left is not None
            if self.right:
                rVal +=  self.right.treeString(weight=weight,
                                               nestLevel=nestLevel+1)
            if self.left:
                rVal += idt(nestLevel)
                rVal +=  "else:\n"
                rVal +=  self.left.treeString(weight=weight,
                                              nestLevel=nestLevel+1)
        if self.output:
            rVal += idt(nestLevel)
            rVal +=  "<= %.4f" % (self.output * weight)
            if self.evalReport:
                rVal += "(%s)" % self.evalReport
            rVal += "\n"
        return rVal


def dump_model(modelName, features):
    """ Print a model in pythoneque code

        Args:
            - modelName: The name of the model, will be read from data/modelName_name.txt
            - features: List of features, 0th item corresponding to Ranklib feature 1
                        each feature is an object with a 'name' parameter

    """
    with open('data/{}_model.txt'.format(modelName)) as f:
        ensembleXml = f.read()
        model = MARTModel(ensembleXml, features)
        for tree in model.trees:
            weight = tree[0]
            tree = tree[1]
            print(tree.treeString(weight=weight))

def eval_model(modelName, features, judgments):
    """ Evaluate a model relative to a list of judgments,
        return a model """

    judgmentList = [judgment for judgment in judgments]
    with open('data/{}_model.txt'.format(modelName)) as f:
        ensembleXml = f.read()
        model = MARTModel(ensembleXml, features)
        model.eval(judgmentList)
        return model
