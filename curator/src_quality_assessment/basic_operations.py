# Software Name: basic_operations.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import json, requests, csv
from geopy import distance
import numpy as np
import pandas as pd
from pathlib import Path
from datetime import datetime
from dateutil import parser
import configparser

LONGITUDE = [-3.883333, -3.7625]; LATITUDE = [43.425, 43.481944]

# Get variables from config file
config = configparser.ConfigParser(); config.read("./general.conf")

santander_coords = config.get('curator','SANTANDER_COORDS').split(",")
santander_latitude = santander_coords[0]; santander_longitude = santander_coords[1]
santander_airport_coords = config.get('curator','SANTANDER_AIRPORT_COORDS').split(",")
santander_airport_latitude = santander_airport_coords[0]; santander_airport_longitude = santander_airport_coords[1]


def get_timestamp(date):
  try:
    return parser.parse(date).timestamp()
  except:     
    return date.timestamp()

def get_date(date):
  return parser.parse(date)

def get_minutes(date):
  try:
    return parser.parse(date).minute
  except:     
    return date.minute

def get_hour(date):
  try:
    return parser.parse(date).hour
  except:     
    return date.hour

def weighted_mean(a, b, alpha):
  return (alpha*a+(1-alpha)*b)

def arithmetic_mean(a):
  return sum(a) / len(a)

def euclidean_distance(a, b):
  return np.linalg.norm(a-b)

def check_coordinates(longitude, latitude): 
  return (LONGITUDE[0] <= longitude <= LONGITUDE[1] and LATITUDE[0] <= latitude <= LATITUDE[1])

def get_aemet_value(entity_date_string, coords, attribute):
  coordinates = (coords[1], coords[0])

  santander_coordinates = (santander_latitude, santander_longitude)
  santander_airport_coordinates = (santander_airport_latitude, santander_airport_longitude)

  distance_santander = distance.distance(santander_coordinates, coordinates).km
  distance_santanderairport = distance.distance(santander_airport_coordinates, coordinates).km

  total = distance_santander + distance_santanderairport
  weigth_santander = 1 - (distance_santander/total)
  
  # List attributes
  # attribute = 0 --> Temperature
  # attribute = 1 --> RelativeHumidity
  listAttibutes = [1,9]

  entity_date_object = pd.to_datetime(entity_date_string, format = "ISO8601")
  current_minutes = get_minutes(entity_date_object)
  current_hour = get_hour(entity_date_object)

  #Estacion AEMET Santander Ciudad
  santander_error = False
  santander_file = "src_quality_assessment/files/ultimosdatos_1111X_datos-horarios.csv"
  if not Path(santander_file).exists(): # file doesn't exist
    url = "https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_1111X_datos-horarios.csv?k=can&l=1111X&datos=det&w=0&f=temperatura&x=h24"
    payload = {}; headers = {}
    try:
      response = requests.request("GET", url, headers=headers, data=payload)
    except:
      santander_error = True
    
    if not santander_error:
      if response.status_code != 200:
        santander_error = True
      else:
        open(santander_file, 'wb').write(response.text.encode('latin_1'))

     
  else:
    file_hour = get_hour(datetime.utcfromtimestamp(Path(santander_file).stat().st_mtime))
    if current_minutes >= 30 and file_hour <= current_hour:
      url = "https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_1111X_datos-horarios.csv?k=can&l=1111X&datos=det&w=0&f=temperatura&x=h24"
      payload={}
      headers = {}
      try:
        response = requests.request("GET", url, headers=headers, data=payload)
      except:
        santander_error = True
      
      if not santander_error:
        if response.status_code != 200:
          santander_error = True
        else:
          open(santander_file, 'wb').write(response.text.encode('latin_1'))

  if not santander_error:  
    i=0
    with open(santander_file, encoding = 'latin_1') as csv_file:
      reader = csv.reader(csv_file, delimiter=',')
      for row in reader:
        if i==4:
          try:
            value1 = float(row[listAttibutes[attribute]])
          except:
            santander_error = True
          break
        i=i+1  


  #Estacion AEMET Santander Aeropuerto
  santander_airport_error = False
  santander_airport_file = "src_quality_assessment/files/ultimosdatos_1109X_datos-horarios.csv"
  if not Path(santander_airport_file).exists(): # file doesn't exist
    url = "https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_1109X_datos-horarios.csv?k=can&l=1109X&datos=det&w=0&f=temperatura&x=h24"
    payload={}
    headers = {}
    try:
      response = requests.request("GET", url, headers=headers, data=payload)
    except:
      santander_airport_error = True
  
    if not santander_airport_error:
      if response.status_code != 200:
        santander_airport_error = True
      else:
        open(santander_airport_file, 'wb').write(response.text.encode('latin_1'))

  else:
    file_hour = get_hour(datetime.utcfromtimestamp(Path(santander_airport_file).stat().st_mtime))
    if current_minutes >= 30 and file_hour <= current_hour:
      url = "https://www.aemet.es/es/eltiempo/observacion/ultimosdatos_1109X_datos-horarios.csv?k=can&l=1109X&datos=det&w=0&f=temperatura&x=h24"
      payload={}; headers = {}
      try:
        response = requests.request("GET", url, headers=headers, data=payload)
      except:
        santander_airport_error = True
    
      if not santander_airport_error:
        if response.status_code != 200:
          santander_airport_error = True
        else:
          open(santander_airport_file, 'wb').write(response.text.encode('latin_1'))
  
  if not santander_airport_error:
    i=0
    with open(santander_airport_file, encoding = 'latin_1') as csv_file:
      reader = csv.reader(csv_file, delimiter=',')
      for row in reader:
        if i==4:
          try:
            value2 = float(row[listAttibutes[attribute]])
          except:
            santander_airport_error = True
          break
        i=i+1  

  if santander_error:
    if santander_airport_error:
      value = False
    else:
      value = value2
  else:
    value = round(weighted_mean(value1, value2, weigth_santander),2)

  return value