# Crocodile Dundee and Rocky have nice Linear Shapes

import numpy
import pylab as pl
import matplotlib as mpl
from ltr.judgments import judgments_to_nparray

norm = mpl.colors.Normalize(0,1.0)

def plot_judgments(qids, xlabel, ylabel, judg_list, focus=None,
                   title_prepend="Features for:"):
    if focus is None:
        focus=qids

    features, predictors, _ = judgments_to_nparray(judg_list)

    from random import shuffle
    from itertools import product
    r = list(range(0,5,1)); shuffle(r)
    g = list(range(0,5,1)); shuffle(g)
    b = list(range(0,5,1)); shuffle(b)

    out_of_focus_alpha=0.1
    in_focus_alpha=0.9

    if len(qids) > 3:
        # Make a random set of colors per query
        colors = [[r*0.1,g*0.1,b*0.1,out_of_focus_alpha] for r,g,b in product(r,g,b)]
        shuffle(colors)
    else: # These are intentionally looking different
        max_c = 0.4
        colors = [[0,max_c,0,out_of_focus_alpha],
                  [max_c,0,0,out_of_focus_alpha],
                  [0,0,max_c,out_of_focus_alpha]]

    qid_col=predictors[:,1]
    qid_idxs=numpy.array([])
    kws = []
    markers=('.', 'P') # Negative / Positive relevance markers...
    legend_paths=[]
    legend_labels=[]
    for idx, qid in enumerate(qids):
        qid_idxs=numpy.argwhere(qid_col==qid).ravel().astype(int)
        judgment=judg_list[qid_idxs[-1].item()]
        kws.append(judgment.keywords)
        x_qidA = features[qid_idxs]
        x_qidA
        y_qidA = predictors[qid_idxs, 0]
        color = colors[idx]
        if qid in focus:
            color[3] = in_focus_alpha
        for grade in [1,0]:
            this_grade=numpy.argwhere(y_qidA==grade)
            path = pl.scatter(x_qidA[this_grade,0],
                              x_qidA[this_grade,1],
                               marker=markers[grade],
                               facecolors=color,
                               edgecolors=color,
                               norm=norm)
            legend_paths.append(path)
            if grade == 0:
                legend_labels.append(judgment.keywords + " irrelevant movie")
            else:
                legend_labels.append(judgment.keywords + " relevant movie")



    pl.title(title_prepend + " {:.25}".format(", ".join(kws)))
    pl.xlabel(xlabel=xlabel)
    pl.ylabel(ylabel=ylabel)
    pl.legend(legend_paths, legend_labels, loc='lower center',
              bbox_to_anchor=[0.5,-0.5])
    pl.savefig('fig.png', dpi=300, bbox_inches='tight')

#plot_all(predictors)

def plot_pairwise_data(features, predictors, title,
                       graph_features=[0,1],
                       xlabel="Delta Title BM25",
                       ylabel="Delta Overview BM25"):
    legend_paths=[]
    for pred in [-1,1]:
        if pred == -1:
            marker = '.'
        elif pred == 1:
            marker = '+'
        path = pl.scatter(features[predictors==pred, graph_features[0]],
                          features[predictors==pred, graph_features[1]],
                           marker=marker)
        legend_paths.append(path)


    pl.title(title)
    pl.xlabel(xlabel=xlabel)
    pl.ylabel(ylabel=ylabel)
    pl.legend(legend_paths, ["Irrelevant minus Relevant", "Relevant minus Irrelevant"], loc='lower center',
              bbox_to_anchor=[0.5,-0.5])
    pl.savefig('all_relevances.png', bbox_inches='tight', dpi=600)
