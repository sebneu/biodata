import argparse
import xml.etree.ElementTree as ET
import gzip

import pymongo
from pymongo import MongoClient

NS = '{http://www.ebi.ac.uk/biosamples/SampleGroupExport/1.0}'

def read_ebi(client):
    i = 0
    db = client.ebi
    with gzip.open('metadata/ebi_metadata.xml.gz', 'rb') as f:

        data = ET.iterparse(f, events=("start", "end"))

        # get the root element
        event, root = next(data)

        for event, elem in data:
            if event == "start" and elem.tag == NS + "BioSample":
                sample_id = elem.attrib['id']

                for attribute in elem.iter(NS + 'Property'):
                    attr = attribute.attrib
                    attr['sample_id'] = sample_id

                    attr['values'] = []
                    for qv in attribute.iter(NS + 'QualifiedValue'):
                        tmp_v = {}
                        v = qv.find(NS + 'Value')
                        if v != None:
                            tmp_v['value'] = v.text

                        term = qv.find(NS + 'TermSourceREF')
                        if term != None:
                            tmp_v['TermSourceREF'] = {}
                            for k in ['Name', 'URI', 'TermSourceID']:
                                k_v = term.find(NS + k)
                                if k_v != None:
                                    tmp_v['TermSourceREF'][k] = k_v.text

                        attr['values'].append(tmp_v)
                    db.properties.insert_one(attr)
                    i += 1
                    if i % 1000000 == 0:
                        print('Inserted EBI properties: ' + str(i))
                root.clear()


def read_ncbi(client):
    i = 0
    db = client.ncbi
    with gzip.open('metadata/ncbi_metadata.xml.gz', 'rb') as f:

        data = ET.iterparse(f, events=("start", "end"))

        # get the root element
        event, root = next(data)

        for event, elem in data:
            if event == "start" and elem.tag == "BioSample":
                sample_id =  elem.attrib['id']

                for attribute in elem.iter('Attribute'):
                    attr = attribute.attrib
                    attr['value'] = attribute.text
                    attr['sample_id'] = sample_id
                    db.attributes.insert_one(attr)
                    i += 1
                    if i % 1000000 == 0:
                        print('Inserted NCBI properties: ' + str(i))
                root.clear()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Store EBI and NCBI bio-datasets in mongodb')
    parser.add_argument('--host', help='mongodb host, default: localhost', default='localhost')
    parser.add_argument('--port', help='mongodb port, default: 27017', default=27017, type=int)
    parser.add_argument('--db', help='bio-db: "ebi" or "ncbi"')
    args = parser.parse_args()
    client = MongoClient(args.host, args.port)
    if args.db == 'ebi':
        read_ebi(client)
    elif args.db == 'ncbi':
        read_ncbi(client)
    else:
        print('DB not supported: ' + args.db)
