# Software Name: geoLinker.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import extractor, pytz, os, configparser
from datetime import datetime
from control_loop import ControlLoopHandler

PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
starting_value = config.getint('params','DISTANCE')
det_id = "uc_geolinker"
det_clh = ControlLoopHandler(det_id,{'distance': starting_value})
det_clh.start()

def findk(k,tofind):
	if k in tofind: return k,True
	for key in tofind:
		if key.endswith('/'+k): return key,True
	return None,False

def link(medida,medida_tosend):
	loc,flag = findk("location",medida)
	if (not flag): return medida_tosend
	distancia = det_clh.get_param('distance')
	if (type(distancia) != int):
		try: distancia = int(distancia)
		except: distancia = starting_value
	lista, sc = extractor.extractbyLocation(medida[loc]["value"], distancia) #list of entities within range
	if (sc >= 400): return medida_tosend
	idlist = list()
 
	dtime = datetime.now().astimezone(pytz.utc)
	obsat = str(dtime).replace(' ','T').replace('+00:00','Z')

	for medida_dev in lista:
		if medida_dev["id"] == medida["id"]: continue
		idlist.append(medida_dev["id"])

	# only update if there are changes
	if len(idlist) != 0:
		if len(idlist) == 1: idlist = idlist[0]
		key,flag = findk("closeTo",medida)
		if flag:
			if (not (set(medida[key]["object"]) == set(idlist))):
				medida_tosend[key] = {"type":"Relationship", "object":idlist, "distance":{"type":"Property", "value":distancia, "unitCode":"MTR"},"observedAt":obsat}
		else: medida_tosend["closeTo"] = {"type":"Relationship", "object":idlist, "distance":{"type":"Property", "value":distancia, "unitCode":"MTR"},"observedAt":obsat}

	return medida_tosend

if __name__ == '__main__':
	medida = {
	"id": "urn:ngsi-ld:BikeHireDockingStation:santander:1",
	"location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [
                -3.8021,
                43.4617
            ]
        },
        "observedAt": "2022-05-30T14:55:01Z"
    }
    }
	medida_linked = link(medida)
	#print(json.dumps(medida_linked, indent=3))
