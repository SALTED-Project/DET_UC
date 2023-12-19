# Software Name: barcelona_bicis.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import json, requests, pytz
from datetime import datetime
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
      res = requests.get('https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_information', headers={'Connection':'close', 'Accept': 'application/json'})
      data = json.loads(res.text)
      datalist_station = data["data"]["stations"]
      res.close()
      res = requests.get('https://api.bsmsa.eu/ext/api/bsm/gbfs/v2/en/station_status', headers={'Connection':'close', 'Accept': 'application/json'})
      data = json.loads(res.text)
      datalist_state = data["data"]["stations"]
      res.close()
    except:
      logger.error("api.bsmsa.eu no responde")
      return
      
    tosend = list()

    # for every measurement
    for state in datalist_state:
      if s_config.exit_event.is_set(): break
      # correlate data
      identifier = state["station_id"]
      station = next((x for x in datalist_station if x["station_id"] == identifier), None)
      if station == None:
        continue

      for k,v in state.items():
        station[k] = v

      #fix location, timestamp and id
      station["location"] = {"type": "Point", "coordinates": [station["lon"],station["lat"]]}
      del station["lon"]
      del station["lat"]

      dtime = datetime.fromtimestamp(station["last_reported"])
      d_utc = dtime.astimezone(pytz.utc)
      newdate = str(d_utc).replace(' ','T')
      newdate = newdate.replace('+00:00','Z')
      station["time_observed"] = newdate
      del station["last_reported"]

      station["station_id"] = "barcelona:"+str(station["station_id"])
      station["type-tag-salted"] = "BikeHireDockingStation"
      tosend.append(station)
      
    # send data to mapper
    if (len(tosend) == 0): return
    m_json = json.dumps(tosend, indent=3)
    res = s.post('http://'+mapper_address+':'+str(mapper_port)+'/UCmapper_batch', data=m_json)
    logger.info("Batch sent")

if __name__ == '__main__':
  s_config.exit_event.clear()
  collect()
  