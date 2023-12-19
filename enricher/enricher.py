# Software Name: enricher.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import injector, generalEnricher, specificEnricher, geoLinker, sensorLinker
from flask import Flask, request
from flask_restful import Api, Resource
import os, inspect, configparser, requests, json, waitress, logging, signal

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
broker_address = config.get('scorpio','SCORPIO_IP')
broker_port = config.get('scorpio','SCORPIO_PORT')
scorpio_dir = "http://"+broker_address+":"+broker_port+"/ngsi-ld/v1/subscriptions/"
enrich_address = config.get('scorpio','ENRICH_CALLBACK')
enrich_port = config.getint('scorpio','ENRICH_PORT')
lista_tipos = config.get('scorpio','TYPES').split(",")
LOG_LEVEL = config.getint('enricher','LOG_LEVEL')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
handler.setLevel(LOG_LEVEL)
logger.setLevel(LOG_LEVEL)
logger.addHandler(handler)

def exit_gracefully(signal, _):
    try: geoLinker.det_clh.stop()
    except: pass
    # delete subscription after quitting
    res = requests.delete(scorpio_dir+"urn:subscription:uc:enricher:stream")
    res.close()
    logger.info("Subscription successfully deleted.")
    exit(0)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

# setup
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__, static_url_path="")
api = Api(app)
session = requests.Session()

def findk(k,tofind):
	if k in tofind: return k,True
	for key in tofind:
		if key.endswith('/'+k): return key,True
	return None,False

class saltedEnricher_generic(Resource):
  def post(self):
    # pre-process data
    request.get_data()
    medida_pre = json.loads(request.data)
    medida_pre = medida_pre["data"][0]
    if "createdAt" in medida_pre: del medida_pre["createdAt"]
    if "modifiedAt" in medida_pre: del medida_pre["modifiedAt"]
    logger.info("Received "+medida_pre["id"])
    tipo = medida_pre["type"].split("#")[-1].split("/")[-1]
    
    # enricher
    medida = generalEnricher.enrich(medida_pre)
    if (tipo in ["AirQualityObserved", "SoundPressureLevel", "Temperature", "TrafficFlowObserved"]): medida = specificEnricher.enrich(medida_pre,medida)
    
    # linker
    medida = sensorLinker.link(medida_pre,medida)
    medida = geoLinker.link(medida_pre,medida)

    if tipo in lista_tipos: context = 'https://raw.githubusercontent.com/SALTED-Project/contexts/main/wrapped_contexts/'+tipo.lower()+'-context.jsonld'
    else: context = 'https://raw.githubusercontent.com/SALTED-Project/contexts/main/wrapped_contexts/default-context.jsonld'
    medida["@context"] = context

    # send data to scorpio
    sc = injector.inject(medida, session)
    
api.add_resource(saltedEnricher_generic, '/saltedEnricher/generic', endpoint='saltedEnricher_generic')

def enrich():
    # prepare headers
    headers_dic = {
        "Accept": "application/ld+json",
        "Content-Type": "application/ld+json"
    }
    
    # prepare subscription
    data_dic = {
        "id": "urn:subscription:uc:enricher:stream",
        "type": "Subscription",
        "description": "Enrich entities of every type when they are updated",
        "entities": [{
                "type": "AirQualityObserved",
                "idPattern": ".*:smartsantander:.*"
            },{
                "type": "BatteryStatus",
                "idPattern": ".*:smartsantander:.*"
            },{
                "type": "ElectroMagneticObserved",
                "idPattern": ".*:smartsantander:.*"
            },{
                "type": "ParkingSpot",
                "idPattern": ".*santander:.*"
            },{
                "type": "SoundPressureLevel",
                "idPattern": ".*:smartsantander:.*"
            },{
               "type": "https://smartdatamodels.org/dataModel.EnergyCIM/Temperature",
               "idPattern": ".*:smartsantander:.*"
            }],
        "notification": {
            "endpoint": {
                    "uri": "http://"+enrich_address+":"+str(enrich_port)+"/saltedEnricher/generic",
                    "accept": "application/json"
            }
        },
        "@context": [
            'https://smartdatamodels.org/context.jsonld',
            'https://raw.githubusercontent.com/smart-data-models/dataModel.Environment/master/context.jsonld',
            'https://raw.githubusercontent.com/smart-data-models/dataModel.Battery/master/context.jsonld',
            'https://raw.githubusercontent.com/smart-data-models/dataModel.OCF/master/context.jsonld',
            'https://raw.githubusercontent.com/smart-data-models/dataModel.Parking/master/context.jsonld'
        ]
    }

    res = requests.delete(scorpio_dir+"urn:subscription:uc:enricher:stream")
    res.close()
    res = requests.post(scorpio_dir, headers=headers_dic, data=json.dumps(data_dic))
    res.close()
    logger.info("Subscribed. Starting server.")
    waitress.serve(app, host="0.0.0.0", port=enrich_port, _quiet=True, connection_limit=1000, cleanup_interval=10, channel_timeout=10, threads=8)

if __name__ == '__main__':
    enrich()
    