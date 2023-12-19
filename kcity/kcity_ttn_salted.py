# Software Name: kcity_ttn_salted.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es>, Juan Ramon SANTANA (Universidad de Cantabria) <jrsantana@tlmat.unican.es> et al.

import sys, os, inspect
import configparser, logging, signal

import paho.mqtt.client as mqtt
import json, requests

############# CONSTANTS ##############

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config2 = configparser.ConfigParser()
config2.read(PROGRAM_PATH + '/general.conf')
mapper_address = config2.get('scorpio','MAPPER_IP')
mapper_port = config2.getint('scorpio','MAPPER_PORT')

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/' + PROGRAM_NAME + '.conf')

# Logging related variables
LOG_LEVEL = config2.getint('collector','LOG_LEVEL')
if not os.path.exists(PROGRAM_PATH + '/logs/'):
    os.makedirs(PROGRAM_PATH + '/logs/')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler_stream = logging.StreamHandler()
handler_stream.setFormatter(formatter)
handler_stream.setLevel(LOG_LEVEL)
handler_file = logging.FileHandler(PROGRAM_PATH + '/logs/kcity.log')
handler_file.setFormatter(formatter)
handler_file.setLevel(logging.INFO)
logger.addHandler(handler_stream)
logger.addHandler(handler_file)
logger.setLevel(logging.DEBUG)

# Get TTN connection variables
TTN_REGION = config.get('ttn','TTN_REGION')
TTN_REGION_PORT = config.getint('ttn','TTN_REGION_PORT')
TTN_USER = config.get('ttn','TTN_USER')
TTN_PASSWORD = config.get('ttn','TTN_PASSWORD')

mqttc = None
def exit_gracefully(signal, _):
  try: mqttc.disconnect()
  except: pass
  logger.info("Gracefully stopped")
  exit(0)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

############# PROGRAM ##############

s = requests.Session()
s.headers.update({"Content-Type": "application/json"})

# On connect callback function
def on_connect(mqttc, obj, flags, rc):
    logger.info('MQTT server connected; rc: ' + str(rc))

# On subscribe callback function
def on_subscribe(mqttc, obj, mid, granted_qos):
    logger.info('MQTT subscription: ' + str(mid) + ' ' + str(granted_qos))

# On message received callback function
def on_message(mqttc, userdata, msg):
    medida = json.loads(msg.payload)
    try:
        if msg.topic[-2:] == 'up':
            if "locations" not in medida["uplink_message"]: logger.debug('Received observation with no locations from device',str(medida["end_device_ids"]["device_id"])); return
            medida_env = dict()
            medida_env["device_id"] = "santander:"+medida["end_device_ids"]["device_id"]
            medida_env["location"] = {'type': 'Point', 'coordinates' : [medida['uplink_message']['locations']['user']['longitude'], medida['uplink_message']['locations']['user']['latitude']]}
            medida_env["dateModified"] = medida["received_at"][:26] + 'Z'
            medida_env["status"] = medida["uplink_message"]["decoded_payload"]["status"]
            if medida_env["status"].startswith("ALIVE"): medida_env["status"] = medida_env["status"].replace("ALIVE_","")
            medida_env["status"] = medida_env["status"].casefold()
            medida_env["category"] = "onStreet"
            medida_env["type-tag-salted"] = "ParkingSpot"

            m_json = json.dumps(medida_env, indent=3)
            # send data to mapper
            res = s.post('http://'+mapper_address+':'+str(mapper_port)+'/UCmapper_stream', data=m_json)
            logger.info('Measurement sent with status code ' + str(res.status_code))
        else:
            logger.debug('OTHER RECV: ' + msg.topic)
            logger.debug(json.dumps(medida, indent=3))
    except Exception as e:
        logger.error('Unknown error: ' + json.dumps(medida))
        logger.exception('Unknown error exception: ' + str(e))
        return  

def connect_mqtt():
    # Init MQTT client
    mqttc = mqtt.Client()

    # Define MQTT callbacks
    mqttc.on_connect = on_connect
    mqttc.on_subscribe = on_subscribe
    mqttc.on_message = on_message

    # Enable encryption
    mqttc.tls_set()	# default certification authority of the system
    mqttc.username_pw_set(TTN_USER, TTN_PASSWORD)

    mqttc.connect(TTN_REGION, TTN_REGION_PORT, 60)
    
    # It subscribes to everything 
    mqttc.subscribe("#", 0)
    
    return mqttc

def main(args):
    
    logger.info('***** PROGRAM ' + PROGRAM_NAME + ' STARTS *****')
    global mqttc
    try:
        mqttc = connect_mqtt()
    except Exception as e:
        logger.error('Something happened while connecting to the TTN MQTT server: ' + str(e))

    try:
        mqttc.loop_forever()
    except KeyboardInterrupt:
        logger.info('Program ended after keyboard interrupt.')
    except Exception as e:
        logger.exception('Something wrong happened: ' + str(e))
    
    mqttc.disconnect()
    return 0

# Call the main function if this program is not being imported
if __name__ == '__main__':
    # main does not access to sys to get the cli arguments, but instead they are passed as parameters.
    sys.exit(main(sys.argv))
