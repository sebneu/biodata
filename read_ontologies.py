import argparse
import os
from elasticsearch import Elasticsearch

from rdflib import Graph
from rdflib import Namespace, OWL, RDF

SKOS = Namespace('http://www.w3.org/2004/02/skos/core#')



def read_ontology(es, filename, pref_label, notation):
    try:
        g = Graph()
        g.parse(pref_label, format='nt')
        g.parse(notation, format='nt')

        for i, c in enumerate(g.subjects()):
            doc = {
                'class_name': str(c),
                'class_name_exact': str(c),
                'ontology': filename.split('--')[0]
            }
            # get last part of URL
            qname = c.rsplit('/', 1)[-1]
            if '#' in qname:
                t = qname.rsplit('#', 1)[-1]
                if len(t) > 0:
                    qname = t
            doc['qname'] = qname
            doc['qname_exact'] = qname

            l = g.value(subject=c, predicate=SKOS.prefLabel)
            if l:
                doc['prefLabel'] = l
                doc['prefLabel_exact'] = l

            n = g.value(subject=c, predicate=SKOS.notation)
            if n:
                doc['notation'] = n
                doc['notation_exact'] = n
            # TODO what about other literals: rdfs:label, dct:title
            index_id = qname
            res = es.index(index="biodata", doc_type='class', id=index_id, body=doc)
            if i % 10000 == 0:
                print('inserted docs: ' + str(i))
    except Exception as e:
        print(e)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Index bioportal ontologies in elasticsearch')
    parser.add_argument('--host', help='elasticsearch host, default: localhost', default='localhost')
    parser.add_argument('--port', help='elasticsearch port, default: 9200', default=9200, type=int)
    args = parser.parse_args()

    es = Elasticsearch([{"host": args.host, "port": args.port}])
    for filename in os.listdir('ontologies'):
        if os.path.isfile('ontologies/' + filename):
            print('Ontology: ' + filename)
            pref_label = 'ontologies/filter/prefLabel_' + filename + '.hdt.nt'
            notation = 'ontologies/filter/notation_' + filename + '.hdt.nt'
            read_ontology(es, filename, pref_label, notation)
