import json
import os

import plotly.graph_objs as go
from plotly.offline import download_plotlyjs, init_notebook_mode, plot, iplot

def log_run(cmd):
    resp = os.popen(cmd).read()
    print(resp)

def quiet_run(cmd):
    os.popen(cmd).read()

def evaluate(mode):
    # Build the docker image
    if mode == 'elastic':
        cmd = 'docker build --no-cache -t ltr-rre rre/elastic/.'
    else:
        cmd = 'docker build --no-cache -t ltr-rre rre/solr/.'

    print('Building RRE image - This will take a while')
    quiet_run(cmd)

    # Remove and run a fresh docker image
    cmd = 'docker rm -f ltr-rre'
    quiet_run(cmd)

    cmd = 'docker run --name ltr-rre ltr-rre'
    print('Running evaluation')
    log_run(cmd)

    # Copy out reports
    cmd = 'docker cp ltr-rre:/rre/target/rre/evaluation.json data/rre-evaluation.json'
    log_run(cmd)

    cmd = 'docker cp ltr-rre:/rre/target/site/rre-report.xlsx data/rre-report.xlsx'
    log_run(cmd)

    print('RRE Evaluation complete')


def rre_table():
    init_notebook_mode(connected=True)

    with open('data/rre-evaluation.json') as src:
        report = json.load(src)
        metrics = report['metrics']

    experiments = ['baseline', 'classic', 'latest']
    precisions = []
    recalls = []
    errs = []

    for exp in experiments:
        precisions.append(metrics['P']['versions'][exp]['value'])
        recalls.append(metrics['R']['versions'][exp]['value'])
        errs.append(metrics['ERR@30']['versions'][exp]['value'])

    trace = go.Table(
            header=dict(values=['', 'Precision', 'Recall', 'ERR'], fill = dict(color='#AAAAAA')),
            cells=dict(values=[
                    experiments,
                    precisions,
                    recalls,
                    errs
                ])
    )

    data = [trace]
    iplot(data)

