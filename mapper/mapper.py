#!flask/bin/python

# Software Name: mapper.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import map_fields
import json, waitress
import configparser, os, logging, inspect, signal
from flask import Flask, request
from flask_restful import Api, Resource
import paho.mqtt.client as mqtt

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
broker_address = config.get('scorpio','SCORPIO_IP')
mqtt_address = config.get('scorpio','MQTT_IP')
mapper_address = config.get('scorpio','MAPPER_IP')
mapper_port = config.getint('scorpio','MAPPER_PORT')
LOG_LEVEL = config.getint('mapper','LOG_LEVEL')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
handler.setLevel(LOG_LEVEL)
logger.setLevel(LOG_LEVEL)
logger.addHandler(handler)

app = Flask(__name__, static_url_path="")
api = Api(app)

def exit_gracefully(signal, _):
  try: client.disconnect()
  except: pass
  logger.info("Gracefully stopped")
  exit(0)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

class UC_mapper_stream(Resource):
  def post(self):
    # get data
    request.get_data()
    medida = json.loads(request.data)

    # get or predict type
    if "type-tag-salted" in medida:
      tipo = medida["type-tag-salted"]
      del medida["type-tag-salted"]
    else:
      tipo = None

    # check if there is info about the units
    if "unit-data-salted" in medida:
      unitData = medida["unit-data-salted"]
      del medida["unit-data-salted"]
    else:
      unitData = None
    
    # map every field based on type
    medida_final = map_fields.mapper(medida,tipo,unitData)
    
    logger.info("Mapped "+medida_final["id"])
    medida_final = json.dumps(medida_final)
    msg_info = client.publish(tipo+"/stream",medida_final)
    msg_info.wait_for_publish()
    
class UC_mapper_batch(Resource):
  def post(self):
    # get data
    request.get_data()
    medidas = json.loads(request.data)
    if len(medidas) == 0: return
    medidas_final = list()
    
    # get or predict type; only once since it's assumed they all belong to the same type
    if "type-tag-salted" in medidas[0]: tipo = medidas[0]["type-tag-salted"]
    else: tipo = None
        
    for medida in medidas:
      if "type-tag-salted" in medida: del medida["type-tag-salted"]
      # check if there is info about the units
      if "unit-data-salted" in medida:
        unitData = medida["unit-data-salted"]
        del medida["unit-data-salted"]
      else:
        unitData = None
      
      # map every field based on type
      medida_final = map_fields.mapper(medida,tipo,unitData)
      medidas_final.append(medida_final)
    
    # send data to scorpio
    if len(medidas_final) == 0: return
    logger.info("Mapped batch")
    medidas_final = json.dumps(medidas_final,indent=3)
    msg_info = client.publish(tipo+"/batch",medidas_final)
    msg_info.wait_for_publish()

api.add_resource(UC_mapper_stream, '/UCmapper_stream', endpoint='UC_mapper_stream')
api.add_resource(UC_mapper_batch, '/UCmapper_batch', endpoint='UC_mapper_batch')

if __name__ == '__main__':
  client = mqtt.Client(client_id="salted_mapper")
  client.connect(mqtt_address,1883,0)
  waitress.serve(app, host=mapper_address, port=mapper_port, _quiet=True, threads=8, connection_limit=1000, cleanup_interval=10, channel_timeout=10)
  client.disconnect()
  