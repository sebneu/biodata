#!/usr/bin/env bash
curl -X POST "localhost:9200/biodata?pretty" -H 'Content-Type: application/json' -d @es_mapping.json