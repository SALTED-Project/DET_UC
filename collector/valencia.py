# Software Name: valencia.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import json, requests
import s_config
import os, inspect, configparser, logging

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
broker_address = config.get('scorpio','SCORPIO_IP')
mapper_address = config.get('scorpio','MAPPER_IP')
mapper_port = config.getint('scorpio','MAPPER_PORT')
LOG_LEVEL = config.getint('collector','LOG_LEVEL')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
handler.setLevel(LOG_LEVEL)
logger.setLevel(LOG_LEVEL)
logger.addHandler(handler)

# prepare headers
headers_dic = {
 "Content-Type": "application/json"
}

def collect():
  with requests.Session() as s:
    s.headers.update(headers_dic)  

    # request data from source
    try:
      res = s.get('https://geoportal.valencia.es/apps/OpenData/Trafico/tra_intensidad_trafico.json?items=100000', headers={'Connection':'close', 'Accept': 'application/json'})
      data = json.loads(res.text)
      datalist_features = data["features"]
      res.close()
    except:
      logger.error("geoportal.valencia.es no responde")
      return
    
    tosend = list()

    # for every measurement
    for feature in datalist_features:
      if s_config.exit_event.is_set(): break
      feature["unit-data-salted"] = dict()
      feature["unit-data-salted"]["properties"] = dict()
      
      feature["properties"]["idtramo"] = "valencia:"+feature["properties"]["idtramo"]

      feature["type-tag-salted"] = "TrafficFlowObserved"
      feature["unit-data-salted"]["properties"]["lectura"] = "vehicles per hour"
      tosend.append(feature)
      
    # send data to mapper
    if (len(tosend) == 0): return
    m_json = json.dumps(tosend, indent=3)
    res = s.post('http://'+mapper_address+':'+str(mapper_port)+'/UCmapper_batch', data=m_json)
    logger.info("Batch sent")

if __name__ == '__main__':
  s_config.exit_event.clear()
  collect()
  