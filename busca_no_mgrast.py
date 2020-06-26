#!/usr/bin/env python3

'''
    Este script faz a busca no MG-RAST por projetos de microbioma Oral.
    O script usa o webservice do MG-RAST que retorna um JSON com os resultados

    Versao 0.1
'''


import json
import requests
import sys

## http://api.mg-rast.org/

MAX_RETURN = '1000'

def busca_json(json, tag):
	ret = ''
	if (tag in json.keys()):
		ret = json[tag]
	else:
		for k in json.keys():
			if (type(json[k]) == dict):
				ret = busca_json(json[k], tag)
				if ret != '':
					break
	return ret

def max_rarefaction(list):
	if not list:
		return '0'
	else:
		return str(max(list)[1])

def ordena_taxon(list):
	if not list:
		linha = ';'
	else:
		temp = sorted(list, key=lambda x: x[1], reverse=True)
		linha = ''
		for g in temp:
			linha += g[0] + ' - ' + str(g[1]) + ';'
	return linha

### Busca por projetos de saliva
response = requests.get("http://api.mg-rast.org/metagenome?material=saliva&limit="+MAX_RETURN)
if (response.status_code == 200):
	# Carrega o objeto retornado
	#dados_da_consulta = json.loads(response.content)
	
	# Busca o total
	#if (dados_da_consulta['total_count'] == 0):
	#	sys.stderr.write('Consulta não retornou resultado\n')
	#	sys.exit(0)
	#else:
	#	sys.stderr.write('Consulta retornou %i resultados\n' % dados_da_consulta['total_count'])

    ## A consulta via API retorna 498 metagenomas
    ## A consulta via web retorna 621 metagenomas
    ## Baixei os Id's dos metagenomas via web (621) e ignorei os registros da API

    ## Abrindo o arquivo
	arquivo_web = open("../all_621_no_trasncriptome.csv").readlines()
	dados_da_consulta = {}
	dados_da_consulta['data'] = []
	for linha in arquivo_web:
		dado = {}
		dado['id'] = linha.split('\t')[1].strip('\n')
		dados_da_consulta['data'].append(dado)

	sys.stderr.write('Consulta retornou %i resultados\n' % len(dados_da_consulta['data']))

	## Formato da Linha
	# MG-RAST_ID; Project_ID; PMID; MATERIAL; LAT; LONG; PI_firstname; PI_lastname; PAIS; QC_failed; QC_unknow; QC_predicted; DOMAIN_bacteria; DOMAIN_eukaryota; TAXON1; TAXON2; TAXON3; TAXON4; TAXON5; CURVE; ALPHA

	recupera = False
	if (len(sys.argv) > 1):
		if (sys.argv[1] == 'recupera=True'):
			recupera = True

	saida = []

	if recupera == True:
		sys.stderr.write("Recuperando...\n")
		file = open('saida.csv').readlines()

		for s in file:
			saida.append(s.split(';')[0])
		tamanho = len(saida)-1
		sys.stderr.write('Ja processados: %d \n' % tamanho)
	
	if recupera == False:	
		print('MG-RAST_ID; Project_ID; PMID; MATERIAL; LAT; LONG; PI_firstname; PI_lastname; PAIS; QC_failed; QC_unknow; QC_predicted; ALPHA; RAREFACTION; SEQUENCE_type; TAXA') 

	for dados in dados_da_consulta['data']:
		if recupera == True:
			if dados['id'] in saida:
				sys.stderr.write('%s ja processado. Passando...\n' % dados['id'])
				continue
		sys.stderr.write('Processando %s\n' % dados['id'])
		linha =  dados['id']
		linha += ';'
		try:
			metadado = requests.get('http://api.mg-rast.org/metagenome?verbosity=full&id=' + dados['id'], timeout=60)
			json_metadado = json.loads(metadado.text)
			#if ('data' in json_metadado.keys()):
			linha += busca_json(json_metadado['data'][0], 'project_id') + ';'
			linha += busca_json(json_metadado['data'][0], 'pmid') + ';'
			linha += busca_json(json_metadado['data'][0],'material') + ';'
			linha += str(busca_json(json_metadado['data'][0],'latitude')) + ';'
			linha += str(busca_json(json_metadado['data'][0],'longitude')) + ';'
			linha += str(busca_json(json_metadado['data'][0],'PI_firstname')) + ';'
			linha += str(busca_json(json_metadado['data'][0],'PI_lastname')) + ';'
			linha += busca_json(json_metadado['data'][0],'country') + ';'
			linha += str(busca_json(json_metadado['data'][0], 'failed_qc')) + ';'
			linha += str(busca_json(json_metadado['data'][0], 'unknown')) + ';'
			linha += str(busca_json(json_metadado['data'][0], 'known_rna')) + ';'
			linha += str(busca_json(json_metadado['data'][0], 'alpha_diversity_shannon')) + ';'
			linha += max_rarefaction(busca_json(json_metadado['data'][0], 'rarefaction')) + ';'
			linha += busca_json(json_metadado['data'][0],'sequence_type') + ';'
			linha += ordena_taxon(busca_json(json_metadado['data'][0], 'genus'))
			sys.stdout.write(linha)
			sys.stdout.write('\n')
			sys.stdout.flush()
		except:
			sys.stderr.write('Timeout em %s\n' % dados['id'])
			sys.stderr.write('Total_count = %s\n' % json_metadado['total_count'])
			sys.stderr.flush()
