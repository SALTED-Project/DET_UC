#!flask/bin/python

# Software Name: sms.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import json, requests, pytz, waitress
from flask import Flask, request
from flask_restful import Api, Resource
#from multiprocessing import Process
from datetime import datetime
from time import sleep
import os, inspect, configparser, logging, signal, threading

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
broker_address = config.get('scorpio','SCORPIO_IP')
sms_callback_address = config.get('scorpio','SMS_CALLBACK')
sms_port = config.getint('scorpio','SMS_PORT')
sms_key = config.get('scorpio','SMS_KEY')
mapper_address = config.get('scorpio','MAPPER_IP')
mapper_port = config.getint('scorpio','MAPPER_PORT')
ONLY_TEMPERATURE = config.getboolean('params','ONLY_TEMPERATURE')
LOG_LEVEL = config.getint('collector','LOG_LEVEL')

if not os.path.exists(PROGRAM_PATH + '/logs/'):
    os.makedirs(PROGRAM_PATH + '/logs/')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler_stream = logging.StreamHandler()
handler_stream.setFormatter(formatter)
handler_stream.setLevel(LOG_LEVEL)
handler_file = logging.FileHandler(PROGRAM_PATH + '/logs/sms.log')
handler_file.setFormatter(formatter)
handler_file.setLevel(logging.INFO)
logger.addHandler(handler_stream)
logger.addHandler(handler_file)
logger.setLevel(logging.DEBUG)

# setup
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
app = Flask(__name__, static_url_path="")
api = Api(app)
complete_meas = dict()
count = dict()
units_airq = {
    "relativeHumidity": "P1",
    "co": "GP",
    "no2": "GQ",
    "o3": "GQ"
}

# renew subscriptions every 160 hours
def renew_subscriptions():
    while(True):
        sleep(3600*160)
        res = requests.patch('https://api.smartsantander.eu:443/v2/subscriptions/'+sms_id_main+'?action=renew', auth=('b941979c29e68f96ed80d875cb7b42d3', ''), verify=False)
        res.close()
        if not ONLY_TEMPERATURE:
            res = requests.patch('https://api.smartsantander.eu:443/v2/subscriptions/'+sms_id_extra+'?action=renew', auth=('b941979c29e68f96ed80d875cb7b42d3', ''), verify=False)
            res.close()
        logger.info("Subscriptions successfully renewed.")

sms_id_main = None
sms_id_extra = None
def exit_gracefully(signal, _):
    # delete sms subscription after quitting
    res = requests.delete('https://api.smartsantander.eu:443/v2/subscriptions/'+sms_id_main, auth=('b941979c29e68f96ed80d875cb7b42d3', ''), verify=False)
    res.close()
    if not ONLY_TEMPERATURE:
        res = requests.delete('https://api.smartsantander.eu:443/v2/subscriptions/'+sms_id_extra, auth=('b941979c29e68f96ed80d875cb7b42d3', ''), verify=False)
        res.close()
    logger.info("Subscriptions successfully deleted.")
    exit(0)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

class saltedGW_generic(Resource):
  def post(self):
    # pre-process data
    request.get_data()
    measurement = json.loads(request.data)
    measurement["urn"] = measurement["urn"][10:]

    try:
        dtime = datetime.strptime(measurement["timestamp"],"%Y-%m-%dT%H:%M:%S.%f%z")
    except:
        dtime = datetime.strptime(measurement["timestamp"],"%Y-%m-%dT%H:%M:%S%z")
    d_utc = dtime.astimezone(pytz.utc)
    newdate = str(d_utc).replace(' ','T')
    newdate = newdate.replace('+00:00','Z')
    measurement["timestamp"] = newdate

    if (measurement["phenomenon"]=="airQuality"):
        measurement["type-tag-salted"] = "AirQualityObserved"
    elif (measurement["phenomenon"]=="batteryLevel"):
        measurement["type-tag-salted"] = "BatteryStatus"
        measurement["unit-data-salted"] = dict()
        measurement["unit-data-salted"]["value"] = "P1"
    elif (measurement["phenomenon"]=="temperature:ambient"):
        measurement["type-tag-salted"] = "Temperature"
        measurement["unit-data-salted"] = dict()
        measurement["unit-data-salted"]["value"] = "CEL"
    elif (measurement["phenomenon"]=="soundPressureLevel:ambient"):
        measurement["type-tag-salted"] = "SoundPressureLevel"
        measurement["unit-data-salted"] = dict()
        measurement["unit-data-salted"]["value"] = "2N"
    elif (measurement["phenomenon"].startswith("presenceState")):
        if measurement["phenomenon"][22:]: measurement["urn"] = measurement["phenomenon"][22:] + ':' + measurement["urn"]
        measurement["type-tag-salted"] = "ParkingSpot"
        if measurement["value"] == 1:
            measurement["value"] = "occupied"
        else:
            measurement["value"] = "free"
        measurement["category"] = "onStreet"
    elif (measurement["phenomenon"].startswith("electricField")):
        measurement["urn"] = measurement["phenomenon"][14:] + ':' + measurement["urn"]
        measurement["type-tag-salted"] = "ElectroMagneticObserved"
        measurement["unit-data-salted"] = dict()
        measurement["unit-data-salted"]["value"] = "C30"

    # send data to mapper
    logger.info("Measurement "+measurement["urn"]+" sent")
    m_json = json.dumps(measurement, indent=3)
    res = requests.post('http://'+mapper_address+':'+str(mapper_port)+'/UCmapper_stream', headers={"Content-Type": "application/json"}, data=m_json)
    res.close()

class saltedGW_AQ(Resource):
  def post(self):
    request.get_data()
    measurement_sms = json.loads(request.data)
    urn = measurement_sms["urn"]

    #map “value”
    if (measurement_sms["phenomenon"]=="relativeHumidity"):
        measurement_sms["relativeHumidity"] = measurement_sms["value"]
    elif (measurement_sms["phenomenon"]=="chemicalAgentAtmosphericConcentration:O3"):
        measurement_sms["o3"] = measurement_sms["value"]
    elif (measurement_sms["phenomenon"]=="chemicalAgentAtmosphericConcentration:NO2"): 
        measurement_sms["no2"] = measurement_sms["value"]
    elif (measurement_sms["phenomenon"]=="chemicalAgentAtmosphericConcentration:CO"):
        measurement_sms["co"] = measurement_sms["value"]
    elif (measurement_sms["phenomenon"]=="chemicalAgentAtmosphericConcentration:airParticles"):
        measurement_sms["airParticles"] = "Total air particles: " + str(measurement_sms["value"]) + " [GP]"

    measurement_sms.pop("value")

    if (urn in complete_meas):
        complete_meas[urn].update(measurement_sms)
        count[urn] += 1
    else:
        complete_meas[urn] = measurement_sms
        count[urn] = 1

    if (count[urn] == 5):
        # send data to generic gw
        complete_meas[urn]["phenomenon"] = "airQuality"
        complete_meas[urn]["unit-data-salted"] = units_airq
        res = requests.post('http://localhost:'+str(sms_port)+'/saltedGW/generic', headers={"Content-Type": "application/json"}, data=json.dumps(complete_meas[urn]))
        res.close()

        complete_meas.pop(urn, None)
        count.pop(urn, None)

api.add_resource(saltedGW_generic, '/saltedGW/generic', endpoint='saltedGW_generic')
api.add_resource(saltedGW_AQ, '/saltedGW/AQ', endpoint='saltedGW_AQ')

def collect():
    # prepare headers
    headers_dic = {
        "Accept": "application/json"
    }

    if ONLY_TEMPERATURE:
        # prepare subscription only_temperature
        sub_main = {
            "target": {
                "technology": "http",
                "parameters": {
                    "url": "http://"+sms_callback_address+":"+str(sms_port)+"/saltedGW/generic"
                }
            },
            "query": {
                "what": {
                    "format": "measurement",
                    "_anyOf": [
                        {"phenomenon": "temperature:ambient"},
                    ]
                }
            }
        }
        
    else:
        # prepare subscription 1
        sub_main = {
            "target": {
                "technology": "http",
                "parameters": {
                    "url": "http://"+sms_callback_address+":"+str(sms_port)+"/saltedGW/generic"
                }
            },
            "query": {
                "what": {
                    "format": "measurement",
                    "_anyOf": [
                        {"phenomenon": "batteryLevel"},
                        {"phenomenon": "temperature:ambient"},
                        {"phenomenon": "soundPressureLevel:ambient"},
                        {"phenomenon": "presenceState:parking"},
                        {"phenomenon": "presenceState:parking:ir"},
                        {"phenomenon": "presenceState:parking:magnetic"},
                        {"phenomenon": "electricField:2100mhz"},
                        {"phenomenon": "electricField:2400mhz"},
                        {"phenomenon": "electricField:1800mhz"},
                        {"phenomenon": "electricField:900mhz"}
                    ]
                }
            }
        }

        # prepare subscription 2
        sub_extra = {
            "target": {
                "technology": "http",
                "parameters": {
                    "url": "http://"+sms_callback_address+":"+str(sms_port)+"/saltedGW/AQ"
                }
            },
            "query": {
                "what": {
                    "format": "measurement",
                    "_anyOf": [
                        {"phenomenon": "relativeHumidity"},
                        {"phenomenon": "chemicalAgentAtmosphericConcentration:O3"},
                        {"phenomenon": "chemicalAgentAtmosphericConcentration:NO2"},
                        {"phenomenon": "chemicalAgentAtmosphericConcentration:CO"},
                        {"phenomenon": "chemicalAgentAtmosphericConcentration:airParticles"}
                    ]
                }
            }
        }


    # request data from source
    res = requests.post('https://api.smartsantander.eu:443/v2/subscriptions?status=active', headers=headers_dic, data=json.dumps(sub_main), auth=(sms_key, ''), verify=False)
    global sms_id_main
    sms_id_main = json.loads(res.text)["id"]
    res.close()
    
    if not ONLY_TEMPERATURE:
        res = requests.post('https://api.smartsantander.eu:443/v2/subscriptions?profile=&status=active', headers=headers_dic, data=json.dumps(sub_extra), auth=(sms_key, ''), verify=False)
        global sms_id_extra
        sms_id_extra = json.loads(res.text)["id"]
        res.close()
        
    logger.info("Program started.")
    renew_th = threading.Thread(target=renew_subscriptions)
    renew_th.daemon = True
    renew_th.start()
    waitress.serve(app, host="0.0.0.0", port=sms_port, _quiet=True, connection_limit=1000, cleanup_interval=10, channel_timeout=10, threads=8)


if __name__ == '__main__':
    collect()
    