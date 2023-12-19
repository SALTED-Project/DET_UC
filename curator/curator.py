# Software Name: curator.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import injector, check_errors, stream_assessment
import json, requests, signal
import paho.mqtt.client as mqtt
import os, inspect, configparser, logging

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
broker_address = config.get('scorpio','SCORPIO_IP')
mqtt_address = config.get('scorpio','MQTT_IP')
LOG_LEVEL = config.getint('curator','LOG_LEVEL')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
loghandler = logging.StreamHandler()
loghandler.setFormatter(formatter)
loghandler.setLevel(LOG_LEVEL)
logger.setLevel(LOG_LEVEL)
logger.addHandler(loghandler)

def exit_gracefully(signal, _):
  try: client.disconnect()
  except: pass
  logger.info("Gracefully stopped")
  exit(0)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

class Handler:
  # always use same HTTP session
  def __init__(self):
    self.session = requests.Session()

  # subscribe on connect
  def on_connect(self, client, userdata, flags, rc):
    if (rc == 0):
      client.subscribe("#",0)
    else:
      logger.error("Connection failed: RC "+str(rc))
      exit()

  def receive_data(self, client, userdata, message):
    try:
      # topic is <type>/<mode>
      tipo = message.topic.split("/")[0]
      modo = message.topic.split("/")[-1]
    except:
      logger.error("Unrecognized topic "+message.topic)
      return
    
    # get data
    data = message.payload.decode("utf-8")
    medidas = json.loads(data)
    if modo == "stream":
      medida_checked, error = check_errors.check(medidas,tipo)
      if error:
        medida_final = medida_checked
      else:
        # assess entity if no errors
        logger.info(medida_checked['id'])
        try:
          medida_final, error = stream_assessment.main(medida_checked, self.session)
          if error: return
        except Exception as exception_error:
          logger.error("ERROR " + str(exception_error) + " -- " + medida_checked['id'])
          return
      # inject entity into the broker
      sc = injector.inject(medida_final, self.session)
    elif modo == "batch":
      medidas_final = list()
      for medida in medidas:
        medida_checked, error = check_errors.check(medida,tipo)
        medidas_final.append(medida_checked)
      # inject entity into the broker
      sc = injector.inject(medidas_final, self.session)

if __name__ == '__main__':
  # begin MQTT subscription
  handler = Handler()
  client = mqtt.Client(client_id="salted_curator")
  client.on_message = handler.receive_data
  client.on_connect = handler.on_connect
  client.connect(mqtt_address,1883,0)

  try:
    client.loop_forever()
  except Exception as exception_error:
    logger.error(exception_error)
    client.disconnect()
    exit()
    