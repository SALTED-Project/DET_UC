# Software Name: injector.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import json, requests
import os, inspect, configparser, logging

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
broker_address = config.get('scorpio','SCORPIO_IP')
broker_port = config.getint('scorpio','SCORPIO_PORT')
LOG_LEVEL = config.getint('curator','LOG_LEVEL')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
handler.setLevel(LOG_LEVEL)
logger.setLevel(LOG_LEVEL)
logger.addHandler(handler)

#scorpio_dir = "http://"+broker_address+":"+str(broker_port)+"/ngsi-ld/v1/entities/"
scorpio_dir_upsert = "http://"+broker_address+":"+str(broker_port)+"/ngsi-ld/v1/entityOperations/upsert?options=update"
headers_post = {
		"Content-Type": "application/ld+json"
	}

def findk(k,tofind):
	if k in tofind: return k,True
	for key in tofind:
		if key.endswith('/'+k): return key,True
	return None,False

# inject single entities or a batch of entities (may be an array of 1 entity)
def inject(medidas, s = requests.Session()):
	is_batch = type(medidas) == type(list())
	if len(medidas) == 0: return 200
	if not is_batch: medidas = [medidas]
	m_json = json.dumps(medidas)
	try: res = s.post(scorpio_dir_upsert, headers=headers_post, data=m_json)
	except: logger.error("Request to Broker failed"); return 400
	sc = res.status_code
	if(sc >= 400): logger.error("HTTP "+str(sc)+" when injecting "+medidas[0]["id"])
	elif is_batch: logger.info("Injected batch, including "+medidas[0]["id"])
	else: logger.info("Injected "+medidas[0]["id"])
	return sc

if __name__ == '__main__':
	medida = '''{
    "id": "urn:TrafficFlowObserved:testV",
    "type": "TrafficFlowObserved",
    "occupancy": {
        "type": "Property",
        "value": 26,
        "observedAt": "2022-05-20T07:42:45Z"
    },
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [
                -3.4391538,
                43.263564
            ]
        }
    },
    "@context": [
      "https://smartdatamodels.org/context.jsonld",
      "https://raw.githubusercontent.com/smart-data-models/dataModel.Transportation/master/context.jsonld"
   ]
}'''
	sc = inject(json.loads(medida))
	#print(sc)
 