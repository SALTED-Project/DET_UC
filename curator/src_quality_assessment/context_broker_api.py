# Software Name: context_broker_api.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import requests, json
from dateutil import parser
from dateutil.relativedelta import relativedelta
import configparser

# TODO: use python-ngsild-client (https://github.com/jlanza/python-ngsild-client)

# Get variables from config file
config = configparser.ConfigParser(); config.read("./general.conf")

broker_ip = config.get('scorpio','SCORPIO_IP'); broker_port = config.getint('scorpio','SCORPIO_PORT')
broker_dir = "http://"+broker_ip+":"+str(broker_port)+"/ngsi-ld/v1/"
base_context = config.get('curator','CONTEXT')
distance_range = config.get('curator','DISTANCE_RANGE')
time_window = config.getfloat('curator','TIME_WINDOW') # minutes
lastN = config.getint('curator','LAST_N')
types = config.get('scorpio','TYPES').split(",")



# upsert_entity: upsert entity into the Context Broker
#     Params: 
#        - body: complete entity body
#     Return: 
#        - status code -- 201 Created / 204 No Content
def upsert_entity(body):
  url = broker_dir + "/entityOperations/upsert?options=update"
  payload = json.dumps([body])
  headers = {
    'Content-Type': 'application/ld+json'
  }

  response = requests.request("POST", url, headers=headers, data=payload)
  # print(response.status_code)
  return response.status_code


# get_entity_by_id: checks if the entity requested already exists in the Context broker and retrieves it
#     Params: 
#        - entity_id: id of the entity requested
#        - entity_type: type of the entity requested
#     Return: 
#        - True/False: Entity found/not found
#        - entity: entity if found
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def get_entity_by_id(entity_id, entity_type):
  if "," in entity_id:
    url = broker_dir + "entities/?id=" + entity_id
  else:
    url = broker_dir + "entities/" + entity_id

  context_link = (
    base_context+entity_type.lower()+'-context.jsonld'
    if entity_type in types
    else base_context + "default-context.jsonld"
  )

  headers = {
    'Accept': 'application/ld+json',
    'Link': '<'+context_link+'>;rel="http://www.w3.org/ns/json-ld#context"'
  }
  payload={}
  response = requests.request("GET", url, headers=headers, data=payload)
  if response.status_code == 200: return True, response.json(), False
  elif response.status_code == 404: return False, None, False
  else: return None, None, True
  


# get_entities_by_type_geoQuery: get entities stored in the Context Broker filtering by type and applying a geoQuery
#     Params: 
#        - entity_type: type requested
#        - coordinates: coordinates to make the geoQuery filter
#     Return: 
#        - array of entities
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def get_entities_by_type_geoQuery(entity_type, coordinates):
  url = broker_dir + "entities/?type="+entity_type+"&georel=near%3BmaxDistance=="+distance_range+"&coordinates="+coordinates+"&geometry=Point"

  context_link = (
    base_context+entity_type.lower()+'-context.jsonld'
    if entity_type in types
    else base_context + "default-context.jsonld"
  )

  headers = {
    'Accept': 'application/ld+json',
    'Link': '<'+context_link+'>;rel="http://www.w3.org/ns/json-ld#context"'
  }
  payload={}

  response = requests.request("GET", url, headers=headers, data=payload)

  if response.status_code == 200: return response.json(), False
  else: return None, True


# get_entity_by_id: get last value recorded in the Context Broker of an entity (by its unique id) or several entities (by a string with a list of ids)
#     Params: 
#        - entity_id: id or list of id concatenated as a string with commas e.g.: id1,id2,id3
#        - entity_type: type of the entities requested
#     Return: 
#        - array of entities
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
# def get_entity_by_id(entity_id, entity_type):
#   url = broker_dir + "entities/?id=" + entity_id

#   context_link = (
#     base_context+entity_type.lower()+'-context.jsonld'
#     if entity_type in types
#     else base_context + "default-context.jsonld"
#   )

#   headers = {
#     'Accept': 'application/ld+json',
#     'Link': '<'+context_link+'>;rel="http://www.w3.org/ns/json-ld#context"'
#   }
#   payload={}

#   response = requests.request("GET", url, headers=headers, data=payload)
#   if response.status_code == 200: return response.json(), False
#   else: return None, True


# get_temporal_values_by_id: get temporal instances of attributes of a determined entity filtered by id and within a time window
#     Params: 
#        - entity_id: id of the requested entity
#        - entity_type: type of the requested entity
#        - entity_date_string: date of the requested entity
#     Return: 
#        - entity with arrays in attributes if there is more thant one temporal instance
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def get_temporal_values_by_id(entity_id, entity_type, entity_date_string):
  entity_date = parser.parse(entity_date_string)
  timeAt = (entity_date - relativedelta(minutes=time_window)).strftime('%Y-%m-%dT%H:%M:%SZ')

  url = broker_dir + "temporal/entities/"+entity_id+"?timerel=after&timeAt="+timeAt+"&lastN="+str(lastN)

  context_link = (
    base_context+entity_type.lower()+'-context.jsonld'
    if entity_type in types
    else base_context + "default-context.jsonld"
  )

  headers = {
    'Accept': 'application/ld+json',
    'Link': '<'+context_link+'>;rel="http://www.w3.org/ns/json-ld#context"'
  }
  payload={}

  response = requests.request("GET", url, headers=headers, data=payload)
  if response.status_code == 200: return response.json(), False
  else: return None, True