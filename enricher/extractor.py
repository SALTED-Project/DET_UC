# Software Name: extractor.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

# LIMITED TO TYPES SPECIFIED IN CONFIG FILE.
# REQUESTING TYPES ENDPOINT EVERY TIME CAUSES TOO MUCH OF AN OVERHEAD.

import json, requests
import os, inspect, configparser

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
broker_address = config.get('scorpio','SCORPIO_IP')
broker_port = config.get('scorpio','SCORPIO_PORT')
type_names = config.get('scorpio','TYPES').split(",")
full_type_names = config.get('scorpio','FULL_TYPES',raw=True).split(",")
number_limit = 1000

scorpio_dir = "http://"+broker_address+":"+broker_port+"/ngsi-ld/v1/entities/"
headers_get = {
		"Accept": "application/json+ld",
		"Link": '<https://smartdatamodels.org/context.jsonld>;rel="http://www.w3.org/ns/json-ld#context";type="application/ld+json"'
	}

sess = requests.Session()

def requestLoop(url,headers):
	nEntities = 0
	nLoops = 0
	entities = list()
	while(nEntities == nLoops * number_limit):
		res = sess.get(url+"&offset="+str(nEntities), headers=headers)
		medidas = res.text
		sc = res.status_code
		if (sc >= 400): return entities, sc
		mjson = json.loads(medidas)
		if type(mjson) != list: mjson = [mjson]
		entities.extend(mjson)
		nLoops += 1
		nEntities += len(mjson)
	return entities, sc

def extractbyID(mid):
	#extract entity
	res = sess.get(scorpio_dir+mid, headers={"Accept": "application/json+ld"})
	medida = res.text
	sc = res.status_code
	mjson = json.loads(medida)

	#remove complete URLs
	msaneada = dict()
	for k,v in mjson.items():
		pos = k.rfind("/")
		if pos == -1:
			msaneada[k] = v
		else:
			newk = k[(pos+1):]
			msaneada[newk] = v

	return msaneada, sc
	#context is not returned
 
def extractbyPattern(tipo,idpattern):
	#extract entities by pattern
	query = "?limit="+str(number_limit)+"&type="+tipo+"&idPattern="+idpattern
	url = scorpio_dir + query
 
	if tipo in type_names: context = 'https://raw.githubusercontent.com/SALTED-Project/contexts/main/wrapped_contexts/'+tipo.lower()+'-context.jsonld'
	else: context = 'https://raw.githubusercontent.com/SALTED-Project/contexts/main/wrapped_contexts/default-context.jsonld'
	headers = {"Accept": "application/json+ld", "Link": '<'+context+'>;rel="http://www.w3.org/ns/json-ld#context";type="application/ld+json"'}
 
	return requestLoop(url,headers)

def extractbySensor(sid):
	#obtain id pattern
	idpattern = ".*:smartsantander:u7jcfa:"+sid
	query = "?limit="+str(number_limit)+"&type="+",".join(full_type_names)+"&idPattern="+idpattern
	url = scorpio_dir + query
	headers = {"Accept": "application/json+ld"}
 
	return requestLoop(url,headers)

def extractbyLocation(loc,dist):
	loc_type = loc["type"]
	loc_coor = loc["coordinates"]
	query = "?limit="+str(number_limit)+"&type="+",".join(full_type_names)+"&georel=near%3BmaxDistance=="+str(dist)+"&coordinates="+json.dumps(loc_coor)+"&geometry="+loc_type
	url = scorpio_dir + query

	return requestLoop(url,headers_get)

def extractbyTypeLocation(loc,tipo,dist):
	loc_type = loc["type"]
	loc_coor = loc["coordinates"]
	query = "?limit="+str(number_limit)+"&type="+tipo+"&georel=near%3BmaxDistance=="+str(dist)+"&coordinates="+json.dumps(loc_coor)+"&geometry="+loc_type
	url = scorpio_dir + query
	
	return requestLoop(url,headers_get)

if __name__ == '__main__':
	peticion = "urn:ngsi-ld:BikeHireDockingStation:santander:1"
	medida, sc = extractbyID(peticion)
	#print(sc,"\n",json.dumps(medida,indent=3))
