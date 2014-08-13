# ES output to TSV script by Dave Withers

import elasticsearch
import csv
import random
import unicodedata

es = elasticsearch.Elasticsearch(["localhost:9200"])

res = es.search(index="asset", body={"fields":["serialNumber","maxEventDate","extraSync"],"query":{"bool":{"must":[{"term":{"summary.extraSync":"tracker"}},{"range":{"summary.maxEventDate":{"from":"1","to":"150781616300000"}}}],"must_not":[],"should":[]}},"from":0,"size":16500,"sort":[],"facets":{}}, size=1600000)

random.seed(1)
sample = res['hits']['hits']

print("Got %d Hits:" % res['hits']['total'])

with open('fleet_nspire_output.tsv', 'wb') as csvfile:
	filewriter = csv.writer(csvfile, delimiter='\t',  
                            quotechar='|', quoting=csv.QUOTE_MINIMAL)

	filewriter.writerow(["serialNumber", "maxEventDate", "extraSync"]) 
	for hit in sample:

		try:
			col1 = hit["fields"]["serialNumber"] 
		except Exception, e:
			col1 = ""
		try:
			col2 = hit["fields"]["maxEventDate"]
		except Exception, e:
			col2 = ""
		try:
			col3 = hit["fields"]["extraSync"].decode('utf-8')
			col3 = col3.replace('\n', ' ')
		except Exception, e:
			col3 = ""
		filewriter.writerow([col1,col2,col3])
