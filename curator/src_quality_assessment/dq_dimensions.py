# Software Name: dq_dimensions.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import math
import numpy as np
from src_quality_assessment import context_broker_api, basic_operations
import configparser

# Get variables from config file
config = configparser.ConfigParser(); config.read("./general.conf")

aemet_measurements_list = config.get('curator','AEMET_MEASUREMENTS_LIST').split(",")
distance_range = config.getfloat('curator','DISTANCE_RANGE')
time_window = config.getint('curator','TIME_WINDOW')


# get_accuracy: calculate the accuracy DQ Dimension against the external trusted source AEMET
#     Params: 
#        - input: input entity being assessed
#     Return: 
#        - accuracy
def get_accuracy(input):
  attribute = aemet_measurements_list.index(input['type'])
  ground_truth = basic_operations.get_aemet_value(input['value']['observedAt'], input['location']['value']['coordinates'], attribute)

  accuracy = (
    round(abs(input['value']['value'] - ground_truth),2)
    if ground_truth
    else "N/A"
  )
    
  return accuracy


# get_precision: calculate the precision DQ Dimension against the surrounding values within a certain range (distance)
#     Params: 
#        - input: input entity being assessed
#     Return: 
#        - precision
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def get_precision(input):
  data_entities, error = context_broker_api.get_entities_by_type_geoQuery(input['type'], str(input['location']['value']['coordinates'])); 
  if error: return None, True

  if len(data_entities) == 0:
    precision = 0 # +-0 degreeCelsius -- 100%
  else:
    quality_id_string_list=""
    for i in data_entities:
      if "hasQuality" in i:
        quality_id_string_list = quality_id_string_list + "," + i["hasQuality"]["object"]
    
    if quality_id_string_list == "": return 0, False
    
    _, quality_entities, error = context_broker_api.get_entity_by_id(quality_id_string_list,"DataQualityAssessment"); 
    if error: return None, True
    
    data_entities = sorted(data_entities, key=lambda x:x['id'])
    quality_entities = sorted(quality_entities, key=lambda x:x['id'])

    inliners_values = []
    for (i, j) in zip(data_entities, quality_entities):
      if "outlier" in j:
        if j['outlier']['value']['isOutlier']['value'] == "False":
          inliners_values.append([i['value']['value']])
      else:
        inliners_values.append([i['value']['value']]) # Temperature measurements outside Santander -- Outlier/Synthetic/Accuracy metrics not performed

    input_value = np.array(input['value']['value'])
    inliners_values = np.array(inliners_values)
    
    precision = (
      basic_operations.euclidean_distance(input_value, inliners_values)/math.sqrt(len(inliners_values))
      if len(inliners_values) != 0
      else 0
    )

  return precision, False


# get_completeness: calculate the completeness DQ Dimension within a given time window
#     Params: 
#        - input: input entity being assessed
#     Return: 
#        - completeness
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def get_completeness(input, is_synthetic):
  data_entity_exists, data_entity, error = context_broker_api.get_entity_by_id(input['id'], input['type'])
  if error: return None, True
  if not data_entity_exists: return 1, False
  if 'hasQuality' not in data_entity: return 1, False

  input_date = (
    input['dateObserved']['value']
    if "dateObserved" in input
    else input['dateModified']['value']
  )
  quality_temporal_entity, error = context_broker_api.get_temporal_values_by_id(input['hasQuality']['object'], "DataQualityAssessment", input_date)
  if error: return None, True

  if "completeness" in quality_temporal_entity:
    if "synthetic" in quality_temporal_entity: # Temperature
      n = int(is_synthetic); total = 1
      total = total + len(quality_temporal_entity['completeness'])
      if isinstance(quality_temporal_entity['synthetic'], list): # more than one temporal value
        for i in quality_temporal_entity['synthetic']:
          if i['value']['isSynthetic']['value'] == "True":
            n = n + 1
      else: # just one temporal value
        if quality_temporal_entity['synthetic']['value']['isSynthetic']['value'] == "True":
          n = n + 1
      return round((total-n)/total,3), False

    else: # Other types -- there are not synthetically created entities
      last_timeliness = (
        quality_temporal_entity['timeliness'][0]['value']
        if isinstance(quality_temporal_entity['timeliness'], list)
        else quality_temporal_entity['timeliness']['value']
      )
            
      total = math.ceil(time_window/last_timeliness)
      existing = 1
      if isinstance(quality_temporal_entity['completeness'], list):
        existing = existing + len(quality_temporal_entity['completeness'])
      else:
        existing = existing + 1
      
      if round(existing/total,3) > 1: return 1, False
      return round(existing/total,3), False
      
  else:
    return 0, False
  

# get_timeliness: calculate the timeliness DQ Dimension
#     Params: 
#        - input: input entity being assessed
#     Return: 
#        - timeliness
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def get_timeliness(input):
  data_entity_exists, data_entity, error = context_broker_api.get_entity_by_id(input['id'], input['type']); 
  if error: return None, True
  if not data_entity_exists: return 10, False

  quality_entity_exists, quality_entity, error = context_broker_api.get_entity_by_id(input['hasQuality']['object'], "DataQualityAssessment"); 
  if error: return None, True
  if not quality_entity_exists: return 10, False

  input_timestamp = (
    basic_operations.get_timestamp(input['dateObserved']['value'])
    if "dateObserved" in input
    else basic_operations.get_timestamp(input['dateModified']['value'])
  )

  data_timestamp = (
    basic_operations.get_timestamp(data_entity['dateObserved']['value'])
    if "dateObserved" in data_entity
    else basic_operations.get_timestamp(data_entity['dateModified']['value'])
  )
  if "synthetic" in quality_entity: # get last no-synthetic entity so the algorithm can be adaptative (maybe the ratio of measurements change)
    input_date = (
      input['dateObserved']['value']
      if "dateObserved" in input
      else input['dateModified']['value']
    )
    quality_temporal_entity, error = context_broker_api.get_temporal_values_by_id(input['hasQuality']['object'], "DataQualityAssessment", input_date)
    if error: return None, True
    
    if isinstance(quality_temporal_entity['synthetic'], list): # more than one temporal value
      for i in quality_temporal_entity['synthetic']:
        if i['value']['isSynthetic']['value'] == "False":
          data_timestamp = basic_operations.get_timestamp(i['observedAt'])
          break

  # input_timestamp = basic_operations.get_timestamp(input['dateModified']['value'])
  # data_timestamp = basic_operations.get_timestamp(data_entity['dateModified']['value'])
  
  input_timeliness = round((input_timestamp - data_timestamp)/60, 2)
  data_timeliness = quality_entity['timeliness']['value']
  
  timeliness = (
    basic_operations.weighted_mean(data_timeliness,input_timeliness,0.8)
    if input_timeliness > 0.5
    else data_timeliness
  )


    #------- WEIGHTED MEAN -------
    # timeliness = basic_operations.weighted_mean(data_timeliness,input_timeliness,0.8)
    #-------------------------------

    #------- ARITHMETIC MEAN -------
    # valueTemporal = getTemporalEntityByID(inputData['id'],False)
    # numSamples = len(valueTemporal['value'])
    # result = arithmeticMean(lastFrequency, rawFrequency, numSamples)
    #-------------------------------
  
  return timeliness, False


# dq_dimensions: perform the calculation of the DQ Dimensions specified
#     Params: 
#        - input: input entity being assessed
#        - is_synthetic: boolean indicating whether the entity being assessed has been synthetically created or not
#        - list_of_dimensions: array containing the DQ Dimensions to be performed
#     Return: 
#        - dimensions: dictionary containing the results of the DQ Dimensions calculated
#    
def dq_dimensions(input, is_synthetic, list_of_dimensions):
  dimensions = {}
  if "accuracy" in list_of_dimensions:
    dimensions['accuracy'] = get_accuracy(input)

  if "precision" in list_of_dimensions:
    dimensions['precision'], error = get_precision(input)
    if error: return None, True

  if "timeliness" in list_of_dimensions:
    dimensions['timeliness'], error = get_timeliness(input)
    if error: return None, True

  if "completeness" in list_of_dimensions:
    dimensions['completeness'], error = get_completeness(input, is_synthetic)
    if error: return None, True

  return dimensions, False