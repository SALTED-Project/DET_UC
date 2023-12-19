# Software Name: santander_buses.py
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
    #! it actually updates every minute, but I would set it up every 5 min not to overload the broker (unless you need it)
    try:
      res = requests.get('http://datos.santander.es/api/rest/datasets/control_flotas_posiciones.json', headers={'Connection':'close', 'Accept': 'application/json'})
      data = json.loads(res.text)
      datalist = data["resources"]
      res.close()
    except:
      logger.error("datos.santander.es no responde")
      return
      
    buses = list()
    tosend = list()

    # for every measurement
    for bus in datalist:
      if s_config.exit_event.is_set(): break
      del bus["ayto:indice"]
      uri = bus["uri"]
      del bus["uri"]
      if(hash(json.dumps(bus)) not in buses): buses.append(hash(json.dumps(bus))) # ignore the duplicates
      else: continue
      bus["uri"] = uri # recover uri after hashing without it

      # prepare id and type
      bus["ayto:vehiculo"] = "santander:"+bus["ayto:vehiculo"]
      bus["type-tag-salted"] = "FleetVehicleStatus"
      
      # prepare unit info
      bus["unit-data-salted"] = dict()
      bus["unit-data-salted"]["ayto:velocidad"] = "KMH"
      bus["ayto:velocidad"] = float(bus["ayto:velocidad"])

      # prepare location and delete fields with no info
      bus["location"] = {"type": "Point", "coordinates": [float(bus["wgs84_pos:long"]),float(bus["wgs84_pos:lat"])]}
      bus["dc:modified"] = bus["ayto:instante"]
      del bus["ayto:instante"]
      del bus["wgs84_pos:long"]
      del bus["wgs84_pos:lat"]
      del bus["gn:coordY"]
      del bus["gn:coordX"]
      del bus["ayto:servicio"]
      del bus["ayto:coche"]
      
      # prepare status field according to sdm:  Enum:'deployed, finished, terminated, servicing, starting'
      if bus["ayto:estado"] == "3": bus["ayto:estado"] = "starting"
      elif bus["ayto:estado"] == "4": bus["ayto:estado"] = "finished"
      elif bus["ayto:estado"] == "5": bus["ayto:estado"] = "servicing"
      elif bus["ayto:estado"] == "6": bus["ayto:estado"] = "terminated"
      else: del bus["ayto:estado"]
      
      tosend.append(bus)
      
    # send data to mapper
    if (len(tosend) == 0): return
    m_json = json.dumps(tosend, indent=3)
    #print(m_json); exit()
    res = s.post('http://'+mapper_address+':'+str(mapper_port)+'/UCmapper_batch', data=m_json)
    logger.info("Batch sent")

if __name__ == '__main__':
  s_config.exit_event.clear()
  collect()
  