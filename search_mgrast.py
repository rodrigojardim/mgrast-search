#!/usr/bin/env python3

'''
	This script searches the MG-RAST portal for Oral microbiome projects. 
	The script uses the MG-RAST webservice
    
	Arguments:
		Input: retrieve=True/False
		Output: <STDOUT> csv file

	Use:
		python3 search_mgrast.py 1>output.csv 2>errors.csv &
		tail -f errors.csv

    Rodrigo Jardim

	Jun 2020    
    Version 0.1
'''


import json
import requests
import sys

## http://api.mg-rast.org/

MAX_RETURN = '1000'
API_URL = 'http://api.mg-rast.org/'
SEARCH_TERM = 'metagenome?material=saliva'

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
		line = ';'
	else:
		temp = sorted(list, key=lambda x: x[1], reverse=True)
		line = ''
		for g in temp:
			line += g[0] + ' - ' + str(g[1]) + ';'
	return line

### Search projects: material = saliva
url = API_URL + SEARCH_TERM + "&limit=" + MAX_RETURN
response = requests.get(url)

if (response.status_code == 200):
    ## Open file
	file_csv = open("data/all_621.csv").readlines()
	data_of_query = {}
	data_of_query['data'] = []
	for line in file_csv:
		data = {}
		data['id'] = line.split(';')[1].strip('\n')
		data_of_query['data'].append(data)

	sys.stderr.write('Query returned %i results\n' % len(data_of_query['data']))

	## Line format
	# MG-RAST_ID; Project_ID; PMID; MATERIAL; LAT; LONG; PI_firstname; PI_lastname; PAIS; QC_failed; QC_unknow; QC_predicted; DOMAIN_bacteria; DOMAIN_eukaryota; TAXON1; TAXON2; TAXON3; TAXON4; TAXON5; CURVE; ALPHA

	retrieve = False
	if (len(sys.argv) > 1):
		if (sys.argv[1] == 'retrieve=True'):
			retrieve = True

	output = []

	if retrieve == True:
		sys.stderr.write("retrieve...\n")
		file = open('output.csv').readlines()

		for s in file:
			output.append(s.split(';')[0])
		size = len(output)-1
		sys.stderr.write('Processed: %d \n' % size)
	
	if retrieve == False:	
		print('MG-RAST_ID; Project_ID; PMID; MATERIAL; LAT; LONG; PI_firstname; PI_lastname; PAIS; QC_failed; QC_unknow; QC_predicted; ALPHA; RAREFACTION; SEQUENCE_type; TAXA') 

	for data in data_of_query['data']:
		if retrieve == True:
			if data['id'] in output:
				sys.stderr.write('%s processed. Skipping...\n' % data['id'])
				continue
		sys.stderr.write('Processing %s\n' % data['id'])
		line =  data['id']
		line += ';'
		try:
			metadado = requests.get('http://api.mg-rast.org/metagenome?verbosity=full&id=' + data['id'], timeout=60)
			json_metadado = json.loads(metadado.text)
			#if ('data' in json_metadado.keys()):
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
			line += sort_taxon(get_json(json_metadado['data'][0], 'genus'))
			sys.stdout.write(line)
			sys.stdout.write('\n')
			sys.stdout.flush()
		except:
			sys.stderr.write('Timeout em %s\n' % data['id'])
			sys.stderr.write('Total count = %s\n' % json_metadado['total_count'])
			sys.stderr.flush()
