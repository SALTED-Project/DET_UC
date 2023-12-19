# Software Name: sensorLinker.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import json, pytz, extractor
from datetime import datetime

def findk(k,tofind):
	if k in tofind: return k,True
	for key in tofind:
		if key.endswith('/'+k): return key,True
	return None,False

def link(medida,medida_tosend):
	pos = medida["id"].rfind("smartsantander")
	if pos == -1: return medida_tosend

	fields = medida["id"].split(":")
	tipo_m = fields[2]

	sid = fields[-1]
	lista, sc = extractor.extractbySensor(sid) #list of entities
	if (sc >= 400): return medida_tosend
 
	dtime = datetime.now().astimezone(pytz.utc)
	obsat = str(dtime).replace(' ','T').replace('+00:00','Z')
	
	idlist = list()
	paralist = list()
	for medida_dev in lista:
		if medida_dev["id"] == medida["id"]: continue
		fields = medida_dev["id"].split(":")
		tipo = fields[2]
		if tipo_m == tipo:
			paralist.append(medida_dev["id"])
		else:
			idlist.append(medida_dev["id"])

	if len(idlist) != 0:
		if len(idlist) == 1: idlist = idlist[0]
		key,flag = findk("sameDevice",medida)
		if flag: # if the entity already had "sameDevice"
			if not (set(medida[key]["object"]) == set(idlist)):
				medida_tosend[key] = {"type":"Relationship","object":idlist,"observedAt":obsat}
		else: medida_tosend["sameDevice"] = {"type":"Relationship","object":idlist,"observedAt":obsat}
    
	if len(paralist) != 0:
		if len(paralist) == 1: paralist = paralist[0]
		key,flag = findk("parallelTo",medida)
		if flag:
			if not (set(medida[key]["object"]) == set(paralist)):
				medida_tosend[key] = {"type":"Relationship","object":paralist,"observedAt":obsat}
		else: medida_tosend["parallelTo"] = {"type":"Relationship","object":paralist,"observedAt":obsat}

	return medida_tosend

if __name__ == '__main__':
	medida = '''{"id":"urn:ngsi-ld:Temperature:smartsantander:u7jcfa:t254"}'''
	medida_linked = link(json.loads(medida))
	#print(json.dumps(medida_linked, indent=3))
