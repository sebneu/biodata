import operator
from collections import defaultdict
import csv
import argparse

import math
from pymongo import MongoClient
from elasticsearch import Elasticsearch


def get_keys_iter(database, client):
    db = getattr(client, database)

    if database == 'ncbi':
        for d in db.attributes.find():
            if 'attribute_name' in d:
                yield d['attribute_name'], d['sample_id']
    elif database == 'ebi':
        for d in db.properties.find():
            if 'class' in d:
                yield d['class'], d['sample_id']
    else:
        yield None, None


def get_ebi_values_per_field(field, ebi):
    values = []
    for e in ebi.properties.find({'class': field}):
        if 'values' in e:
            for v in e['values']:
                values.append(v['value'])
    return values


def get_ncbi_values_per_field(field, ncbi):
    return [e['value'] for e in ncbi.attributes.find({'attribute_name': field}) if 'value' in e]


def get_values_per_fields(database, client):
    db = getattr(client, database)

    if database == 'ncbi':
        for d in db.attributes.aggregate( [ { "$group" : { "_id" : "$attribute_name" } } ] ):
            values = get_ncbi_values_per_field(d['_id'], db)
            yield d['_id'], values
    elif database == 'ebi':
        for d in db.properties.aggregate( [ { "$group" : { "_id" : "$class" } } ] ):
            values = get_ebi_values_per_field(d['_id'], db)
            yield d['_id'], values
    else:
        yield None, []


# usage of metadata keys
def usage(database, client):
    keys = get_keys_iter(database, client)

    total_keys = set()
    key_frequency = defaultdict(int)
    sample_keys = defaultdict(set)
    i = 0
    for k, sample in keys:
        total_keys.add(k)
        key_frequency[k] += 1
        sample_keys[sample].add(k)
        i += 1
        if i % 1000000 == 0:
            print('processed: ' + str(i))

    print('total keys: ' + str(len(total_keys)))

    # usage for a single key
    k_usage = defaultdict(int)
    for k in total_keys:
        k_usage[k] = key_frequency[k] / len(sample_keys)

    sorted_k_usage = sorted(k_usage.items(), key=operator.itemgetter(1), reverse=True)
    with open('results/' + database + '_key_usage.csv', 'w') as f:
        csvw = csv.writer(f)
        for k, v in sorted_k_usage:
            csvw.writerow([k, str(v)])


    # sample usage
    s_usage = defaultdict(int)
    for sample in sample_keys:
        s_usage[sample] = len(sample_keys[sample]) / len(total_keys)
    sorted_s_usage = sorted(s_usage.items(), key=operator.itemgetter(1), reverse=True)
    with open('results/' + database + '_sample_usage.csv', 'w') as f:
        csvw = csv.writer(f)
        for k, v in sorted_s_usage:
            csvw.writerow([k, str(v)])

    # portal usage = average sample usage
    p_usage = sum(s_usage.values()) / len(s_usage)
    with open('results/' + database + '_portal_usage.csv', 'w') as f:
        f.write(database + ',' + str(p_usage))


def find_exact_mapping(value, es, ontology=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"term": {"qname_exact": value}},
                                {"term": {"notation_exact": value}},
                                {"term": {"prefLabel_exact": value}}
                            ]
                        }
                    }
                ]
            }
        }
    }
    if ontology:
        query["query"]["bool"]["must"].append({"terms": {"ontology": ontology}})
    res = es.search(index="biodata", body=query)

    ontologies = []
    for d in res['hits']['hits']:
        ontologies.append(d['_source']['ontology'])
    return ontologies


def find_matching_ontologies(value, es, ontology=None):
    query = {
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"match": {"qname": value}},
                                {"match": {"notation": value}},
                                {"match": {"prefLabel": value}}
                            ]
                        }
                    }
                ]
            }
        }
    }
    if ontology:
        query["query"]["bool"]["must"].append({"terms": {"ontology": ontology}})
    res = es.search(index="biodata", body=query)

    ontologies = []
    for d in res['hits']['hits']:
        ontologies.append(d['_source']['ontology'])
    return ontologies


def metadata_ontology_mapping(database, client, es):
    for field, values in get_values_per_fields(database, client):
        print('Metadata field: ' + field)
        ontologies = defaultdict(int)
        for v in values:
            if v:
                os = find_exact_mapping(v, es)
                for o in os:
                    ontologies[o] += 1

        sorted_ontologies = sorted(ontologies.items(), key=operator.itemgetter(1), reverse=True)
        with open('results/exact_ontology_mappings.csv', 'a') as f:
            if len(sorted_ontologies) > 0:
                line = [field, str(len(values)), sorted_ontologies[0][0], str(sorted_ontologies[0][1])]
            else:
                line = [field, str(len(values)), '', '']
            csvw = csv.writer(f)
            csvw.writerow(line)


def get_trainingdata_features(field, values, ontologies, es):
    num_values = len(values)
    distinct_values = len(set(values))
    exact = 0
    match = 0
    distinct_exact = set()
    distinct_match = set()
    numbers_exact = 0.0
    numbers_match = 0.0

    ontologies_exact = defaultdict(int)
    ontologies_match = defaultdict(int)

    for v in values:
        if v:
            mo = find_matching_ontologies(v, es, ontology=ontologies)
            if len(mo) > 0:
                ontologies_match[mo[0]] += 1
                match += 1
                distinct_match.add(v)
                numbers = sum(c.isdigit() for c in v)
                numbers_match += (numbers / float(len(v)))

            eo = find_exact_mapping(v, es, ontology=ontologies)
            if len(eo) > 0:
                ontologies_exact[eo[0]] += 1
                exact += 1
                distinct_exact.add(v)
                numbers = sum(c.isdigit() for c in v)
                numbers_exact += (numbers / float(len(v)))

    numbers_exact = numbers_exact/ num_values
    numbers_match = numbers_match / num_values

    sorted_eo = sorted(ontologies_exact.items(), key=operator.itemgetter(1), reverse=True)
    sorted_mo = sorted(ontologies_match.items(), key=operator.itemgetter(1), reverse=True)
    ontology_count_exact = ''
    ontology_exact = ''
    ontology_count_match = ''
    ontology_match = ''
    if len(sorted_eo) > 0:
        ontology_exact = sorted_eo[0][0]
        ontology_count_exact = sorted_eo[0][1]
    if len(sorted_mo) > 0:
        ontology_match = sorted_mo[0][0]
        ontology_count_match = sorted_mo[0][1]

    return [field, num_values, distinct_values, exact, exact / float(num_values), len(distinct_exact),
            numbers_exact, ontology_exact, ontology_count_exact,
            match, match / float(num_values), len(distinct_match),
            numbers_match, ontology_match, ontology_count_match]


def get_ontology_mappings():
    assigned_ontologies = {}
    with open('metadata/attributes.csv', 'r') as f:
        csvr = csv.reader(f)
        for row in csvr:
            if row[1] == 'ontology_term':
                field = row[0]
                ontologies = row[2].split('|')
                assigned_ontologies[field] = ontologies
    return assigned_ontologies


def get_trainingdata_values(client, es):
    assigned_ontologies = get_ontology_mappings()
    # get features:
    # num of values
    # selectivity
    # num of exact matches
    # num of partial matches
    # num of distinct matches
    # ratio of numbers/letters
    with open('results/attribute_mappings_features.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerow(['field', 'total_values', 'distinct_values', 'exact', 'perc_exact', 'distinct_exact',
                       'avg_numbers_exact', 'ontology_exact', 'ontology_count_exact',
                       'match', 'perc_match', 'distinct_match',
                       'avg_numbers_match', 'ontology_match', 'ontology_count_match'])

        for field in assigned_ontologies:
            print('Processing attribute: ' + field + ', ' + '|'.join(assigned_ontologies[field]))
            values = get_ncbi_values_per_field(field, client.ncbi)
            row = get_trainingdata_features(field, values, assigned_ontologies[field], es)
            csvw.writerow(row)


def get_all_field_values(client, es, database='ncbi'):
    filename = 'results/' + database + '_attributes_features.csv'
    with open(filename, 'w') as f:
        csvw = csv.writer(f)
        csvw.writerow(['field', 'total_values', 'distinct_values', 'exact', 'perc_exact', 'distinct_exact', 'avg_numbers_exact', 'ontology_exact', 'ontology_count_exact', 'match', 'perc_match', 'distinct_match', 'avg_numbers_match', 'ontology_match', 'ontology_count_match'])

    for field, values in get_values_per_fields(database, client):
        print('Processing attribute: ' + field)
        row = get_trainingdata_features(field, values, None, es)
        with open(filename, 'a') as f:
            csvw = csv.writer(f)
            csvw.writerow(row)


def get_distinct_values(client, es, database='ncbi'):
    assigned_ontologies = get_ontology_mappings()

    with open('results/distinct_attribute_values.csv', 'w') as f:
        csvw = csv.writer(f)
        csvw.writerow(['field', 'total_values', 'distinct_values'])

        for field in assigned_ontologies:
            print('Processing attribute: ' + field)
            values = get_ncbi_values_per_field(field, client.ncbi)
            row = [field, str(len(values)), str(len(set(values)))]
            csvw.writerow(row)

    filename = 'results/distinct_' + database + '_attributes_values.csv'
    with open(filename, 'w') as f:
        csvw = csv.writer(f)
        csvw.writerow(['field', 'total_values', 'distinct_values'])

    for field, values in get_values_per_fields(database, client):
        print('Processing attribute: ' + field)
        row = [field, str(len(values)), str(len(set(values)))]
        with open(filename, 'a') as f:
            csvw = csv.writer(f)
            csvw.writerow(row)


if __name__ == '__main__':

    parser = argparse.ArgumentParser(prog='Indexing and experiments over bioportal data')
    parser.add_argument('--eshost', default='localhost')
    parser.add_argument('--esport', type=int, default=9200)
    parser.add_argument('--mongohost', default='localhost')
    parser.add_argument('--mongoport', type=int, default=27017)

    subparsers = parser.add_subparsers(help='Get features for all fields')
    # create the parser for the "start" command
    parser_articles = subparsers.add_parser('all-fields')
    parser_articles.set_defaults(func=get_all_field_values)

    parser_articles = subparsers.add_parser('mapping-fields')
    parser_articles.set_defaults(func=get_trainingdata_values)

    parser_articles = subparsers.add_parser('distinct-values')
    parser_articles.set_defaults(func=get_distinct_values)


    # parse arguments
    args = parser.parse_args()

    client = MongoClient(args.mongohost, args.mongoport)
    es = Elasticsearch([{"host": args.eshost, "port": args.esport}])

    args.func(client, es)

    #usage('ncbi', client)
    #metadata_ontology_mapping('ncbi', client, es)
    #get_trainingdata_values(client, es)
    #get_all_field_values(client, es)

