#!/usr/bin/env python3

'''
	This script searches the MG-RAST portal for Oral microbiome projects. 
	The script uses the MG-RAST webservice
    
	Arguments:
		Input: retrieve=True/False
		Output: <STDOUT> csv file

	Use:
		python3 mgrast_search.py -i data/all_621.csv -o output.csv &
		tail -f mgrast.log

		or (to retrieve)
		
		python3 mgrast_search.py -i data/all_621.csv -r True >> output.csv &
		tail -f mgrast.log

    Rodrigo Jardim

	Jun 2020    
    Version 0.1
'''

import argparse
import logging
import json
import requests
import sys

## http://api.mg-rast.org/

logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s', level=logging.INFO)
logging.info('Started')

def get_json(json, tag):
	ret = ''
	if (tag in json.keys()):
		ret = json[tag]
	else:
		for k in json.keys():
			if (type(json[k]) == dict):
				ret = get_json(json[k], tag)
				if ret != '':
					break
	return ret

def max_rarefaction(list):
	if not list:
		return '0'
	else:
		return str(max(list)[1])

def sort_taxon(list):
	if not list:
		taxa = ';'
	else:
		temp = sorted(list, key=lambda x: x[1], reverse=True)
		taxa = ''
		for g in temp:
			taxa += g[0] + ' - ' + str(g[1]) + ';'
	return taxa

ap = argparse.ArgumentParser()

# Add the arguments to the parser
ap.add_argument("-i", "--input", required=True, help="input file. Projects list (csv)")
ap.add_argument("-o", "--output", required=True, help="output file")
ap.add_argument("-r", "--retrieve", required=False, help="if true, retrieve searches", default="false")
args = vars(ap.parse_args())

retrieve = args['retrieve'] == "True"
filePath = args['input']
outPath = args['output']

## Open file
file_csv = open(filePath).readlines()
data_of_query = {}
data_of_query['data'] = []
for line in file_csv:
	data = {}
	data['id'] = line.split(';')[1].strip('\n')
	data_of_query['data'].append(data)

logging.debug('Query returned %i results' % len(data_of_query['data']))

## Line format
# MG-RAST_ID; Project_ID; PMID; MATERIAL; LAT; LONG; PI_firstname; PI_lastname; PAIS; QC_failed; QC_unknow; QC_predicted; DOMAIN_bacteria; DOMAIN_eukaryota; TAXON1; TAXON2; TAXON3; TAXON4; TAXON5; CURVE; ALPHA

output = []

if retrieve == True:
	logging.debug("retrieving...")
	file = open(outPath).readlines()

	for s in file:
		output.append(s.split(';')[0])
	size = len(output)-1
	logging.debug('Processed: %d ' % size)
	out = open(outPath, 'a')
	outTaxa = open(outPath+".taxa.csv", 'a')
else:
	out = open(outPath, 'w')
	outTaxa = open(outPath+".taxa.csv", 'w')


if retrieve == False:	
	out.write('MG-RAST_ID; Project_ID; PMID; MATERIAL; LAT; LONG; PI_firstname; PI_lastname; PAIS; QC_failed; QC_unknow; QC_predicted; ALPHA; RAREFACTION; SEQUENCE_type\n') 

for data in data_of_query['data']:
	if retrieve == True:
		if data['id'] in output:
			logging.info('%s processed. Skipping...' % data['id'])
			continue
	logging.info('Processing %s' % data['id'])
	line =  data['id']
	line += ';'
	try:
		metadado = requests.get('http://api.mg-rast.org/metagenome?verbosity=full&id=' + data['id'], timeout=60)
		json_metadado = json.loads(metadado.text)
		line += get_json(json_metadado['data'][0], 'project_id') + ';'
		line += get_json(json_metadado['data'][0], 'pmid') + ';'
		line += get_json(json_metadado['data'][0],'material') + ';'
		line += str(get_json(json_metadado['data'][0],'latitude')) + ';'
		line += str(get_json(json_metadado['data'][0],'longitude')) + ';'
		line += str(get_json(json_metadado['data'][0],'PI_firstname')) + ';'
		line += str(get_json(json_metadado['data'][0],'PI_lastname')) + ';'
		line += get_json(json_metadado['data'][0],'country') + ';'
		line += str(get_json(json_metadado['data'][0], 'failed_qc')) + ';'
		line += str(get_json(json_metadado['data'][0], 'unknown')) + ';'
		line += str(get_json(json_metadado['data'][0], 'known_rna')) + ';'
		line += str(get_json(json_metadado['data'][0], 'alpha_diversity_shannon')) + ';'
		line += max_rarefaction(get_json(json_metadado['data'][0], 'rarefaction')) + ';'
		line += get_json(json_metadado['data'][0],'sequence_type') + ';'
		out.write(line)
		out.write('\n')
		
		taxa =  data['id']
		taxa += ";"
		taxa += sort_taxon(get_json(json_metadado['data'][0], 'genus'))
		outTaxa.write(taxa)
		outTaxa.write('\n')
		
		logging.info("Processed "+data['id'])
	except:
		logging.error('Timeout em %s' % data['id'])

out.close()
outTaxa.close()

logging.info("Processing Taxonomy file")
fileTaxa = open(outPath+".taxa.csv").readlines()
taxonomy = {}    
for line in fileTaxa:
    line = line.rstrip('\n')
    records = line.split(';')
    mgrastID = records[0]
    total_records = len(records)
    index_begin_taxonomy = 1
    for i in range(index_begin_taxonomy,total_records):
        if ('-' in records[i]):
            taxonomy[records[i].split(' - ')[0]] = records[i].split(' - ')[1]
    
    output = 'mgrastID;'
    for k, v in taxonomy.items():
        output += k + ';'
    output += '\n' + mgrastID + ';'
    for k, v in taxonomy.items():
        output += v + ';'
    output = output[0:len(output)-1] #+ ';'
    logging.debug("Creating "+mgrastID+'.csv')
    file_out = open("data/"+mgrastID+'.csv', 'w')
    file_out.write(output)
    file_out.close()
    logging.info("Created "+mgrastID+'.csv')

logging.info("End")