# Software Name: barcelona.py
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
    try:
      # request csv for locations
      res = requests.get('https://opendata-ajuntament.barcelona.cat/data/dataset/1090983a-1c40-4609-8620-14ad49aae3ab/resource/1d6c814c-70ef-4147-aa16-a49ddb952f72/download/transit_relacio_trams.csv', headers={'Connection':'close'})
      data = res.text
      datalist_csv = data.split("\n")
      datalist_csv.pop(0)
      dict_csv = dict()
      for elem in datalist_csv:
        spl = elem.split(",")
        dict_csv[spl[0]] = elem
      res.close()
    except:
      logger.error("opendata-ajuntatment.barcelona.cat no responde")
      return

    try:
      # request data from source
      res = requests.get('https://www.bcn.cat/transit/dades/dadestrams.dat', headers={'Connection':'close'})
      data = res.text
      datalist_features = data.split("\n")
      datalist_features.pop()
      res.close()
    except:
      logger.error("www.bcn.cat no responde")
      return
    
    tosend = list()

    # for every measurement
    for feature in datalist_features:
      if s_config.exit_event.is_set(): break
      medida = dict()

      fields = feature.split("#")
      medida["id_tramo"] = "barcelona:"+fields[0]
      try:
        location = dict_csv[fields[0]].split("\"")
      except:
        continue

      coords = location[-2].split(",")
      coord_list = list()
      for i in range(0,int(len(coords)/2)):
        coord_list.append([float(coords[i*2]),float(coords[i*2+1])])

      medida["location"] = {"type": "LineString", "coordinates": coord_list}

      dtime = datetime.strptime(fields[1],"%Y%m%d%H%M%S")
      d_utc = dtime.astimezone(pytz.utc)
      newdate = str(d_utc).replace(' ','T')
      newdate = newdate.replace('+00:00','Z')
      medida["time_observed"] = newdate
      
      # medida["estado"] = fields[2]
      # Estat actual (0 = sense dades / 1 = molt fluid / 2 = fluid / 3 = dens / 4 = molt dens / 5 = congestió / 6 = tallat)
      # can be changed to 'congested' (True if 5 or 6/False if 1-4)
      medida["congested"] = True if (int(fields[2]) > 4) else False
      medida["type-tag-salted"] = "TrafficFlowObserved"
      tosend.append(medida)
      
    # send data to mapper
    if (len(tosend) == 0): return
    m_json = json.dumps(tosend, indent=3)
    res = s.post('http://'+mapper_address+':'+str(mapper_port)+'/UCmapper_batch', data=m_json)
    logger.info("Batch sent")

if __name__ == '__main__':
  s_config.exit_event.clear()
  collect()
  