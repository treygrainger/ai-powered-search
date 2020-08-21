# converts LambdaMART XML models to JSON for Solr..

import xml.etree.ElementTree as ET


def convert(ensemble_xml_string, modelName, featureSet, featureMapping):
    modelClass = 'org.apache.solr.ltr.model.MultipleAdditiveTreesModel'

    model = {
        'store': featureSet,
        'name': modelName,
        'class': modelClass,
        'features': featureMapping
    }

    # Clean up header
    ensemble_xml_string = '\n'.join(ensemble_xml_string.split('\n')[7:])
    lambdaModel = ET.fromstring(ensemble_xml_string)

    trees = []
    for node in lambdaModel:
        t = {
            'weight': str(node.attrib['weight']),
            'root': parseSplits(node[0], featureMapping)
        }
        trees.append(t)

    # print(trees)
    model['params'] = {'trees': trees}

    return model

def parseSplits(split, features):
    obj = {}
    for el in split:
        if (el.tag == 'feature'):
            obj['feature'] = features[(int(el.text.strip()) - 1)]['name']
        elif (el.tag == 'threshold'):
            obj['threshold'] = str(el.text.strip())
        elif (el.tag == 'split' and 'pos' in el.attrib):
            obj[el.attrib['pos']] = parseSplits(el, features)
        elif (el.tag == 'output'):
            obj['value'] = str(el.text.strip())
    return obj
