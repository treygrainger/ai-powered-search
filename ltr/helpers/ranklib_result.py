
import re

class RanklibResult:

    """ A result of ranklib training, either for a
        single training operation
        (where trainingLogs is just set, and has a single item)
        or k-folds cross validation
        (where the foldResults/kcv are set; with a result for
         each fold that is run """

    def __init__(self, trainingLogs, foldResults,
                 kcvTestAvg, kcvTrainAvg):
        self.trainingLogs = trainingLogs
        self.foldResults = foldResults
        self.kcvTrainAvg = kcvTrainAvg
        self.kcvTestAvg = kcvTestAvg

class TrainingLog:

    def __init__(self, rounds, impacts, trainMetricName, trainMetricVal):
        self.impacts = impacts
        self.rounds = rounds
        self.trainMetricName = trainMetricName
        self.trainMetricVal = trainMetricVal


    def metric(self):
        if self.trainMetricName is not None:
            return self.trainMetricVal
        if len(self.rounds) > 0:
            return self.rounds[-1]
        else:
            return 0

class FoldResult:
    def __init__(self, foldId, trainMetric, testMetric):
        self.foldNum = foldId
        self.trainMetric = trainMetric
        self.testMetric = testMetric

impactRe = re.compile(' Feature (\d+) reduced error (.*)')
roundsRe = re.compile('(\d+)\s+\| (\d+)')
foldsRe = re.compile('^Fold (\d+)\s+\|(.*)\|(.*)')
avgRe = re.compile('^Avg.\s+\|(.*)\|(.*)')
trainMetricRe = re.compile('(.*@.*) on training data: (.*)')

def parse_training_log(rawResult):
    """ Takes raw result from Ranklib training and
        gathers the feature impacts, training rounds,
        and any cross-validation information """
    lines = rawResult.split('\n')
    # Fold 1	|   0.9396	|  0.8764
    train = False
    logs = []
    folds = []
    impacts = {}
    rounds = []
    trainMetricName = None
    trainMetricVal = 0.0
    kcvTestAvg = kcvTrainAvg = None
    for line in lines:
        if 'Training starts...' in line:
            if train:
                log = TrainingLog(rounds=rounds,
                                  impacts=impacts,
                                  trainMetricName=trainMetricName,
                                  trainMetricVal=trainMetricVal)
                logs.append(log)
            impacts = {}
            rounds = []
            train = True

        if train:
            m = re.match(impactRe, line)
            if m:
                ftrId = m.group(1)
                error = float(m.group(2))
                impacts[ftrId] = error
            m = re.match(roundsRe, line)
            if m:
                values = line.split('|')
                metricTrain = float(values[1])
                rounds.append(metricTrain)
            m = re.match(trainMetricRe, line)
            if m:
                trainMetricVal = float(m.group(2))
                trainMetricName = m.group(1)

        m = re.match(foldsRe, line)
        if m:
            foldId = m.group(1)
            trainMetric = float(m.group(2))
            testMetric = float(m.group(3))
            folds.append(FoldResult(foldId=foldId,
                                    testMetric=testMetric,
                                    trainMetric=trainMetric))
        m = re.match(avgRe, line)
        if m:
            kcvTrainAvg = float(m.group(1))
            kcvTestAvg = float(m.group(2))

    if train:
        log = TrainingLog(rounds=rounds,
                          impacts=impacts,
                          trainMetricName=trainMetricName,
                          trainMetricVal=trainMetricVal)
        logs.append(log)

    return RanklibResult(trainingLogs=logs,
                         foldResults=folds,
                         kcvTrainAvg=kcvTrainAvg,
                         kcvTestAvg=kcvTestAvg)

