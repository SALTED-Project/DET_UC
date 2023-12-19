# Software Name: quality_assessment.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import pandas, math
from src_quality_assessment import context_broker_api, dq_dimensions, anomaly_detection, basic_operations

# do_quality_assessment: performs the Quality Assessment of the entity/input given
#     Params: 
#        - input: input entity being assessed
#        - is_synthetic: boolean specifying if the entity is a missing value and needs to be synthetically created
#     Return: 
#        - quality: body of the quality entity generated
#        - value: synthetic value created (if needed) or current measurement value
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def do_quality_assessment(input, is_synthetic):
	
	# Assess depending on entity type
	if input['type'] == "Temperature": # Extra quality metrics (outlier, synthetic, accuracy and precision)
		if basic_operations.check_coordinates(input['location']['value']['coordinates'][0], input['location']['value']['coordinates'][1]): # Inside Santander
			list_of_dimensions = ['accuracy', 'precision', 'timeliness', 'completeness']
			list_of_processes = ['outlier', 'synthetic']

		else: # Outside Santander
			list_of_dimensions = ['precision', 'timeliness', 'completeness']
			list_of_processes = []

	else: # Not a Temperature entity
		list_of_dimensions = ['timeliness', 'completeness']
		list_of_processes = []
	
	# Anomaly detection + Synthetic value
	outlier = anomaly_detection.novelty_detection(input, is_synthetic, list_of_processes)
	synthetic = anomaly_detection.create_synthetic_value(input, is_synthetic, list_of_processes)
	if is_synthetic: input['value']['value'] = synthetic['synthetic']['value']

	# DQ Dimensions
	dimensions, error = dq_dimensions.dq_dimensions(input, is_synthetic, list_of_dimensions)
	if error: return None, None, True
	
	# Tagging the DataQualityAssessment entity
	attributes = dict(dimensions)
	quality_id = input['hasQuality']['object']
	input_date = (
		input['dateObserved']['value']
		if "dateObserved" in input
		else input['dateModified']['value']
	)
	quality = tagging(attributes, quality_id, input_date)
	
	# Return
	if is_synthetic:
		return quality, input['value']['value'], False

	
	return quality, None, False	


# check_num_of_missing_entities: checks if some measurements have been lost
#     Params: 
#        - input: input entity being assessed
#     Return: 
#        - num_missing: number of missing measurements
#        - data_timeliness: timeliness of the last entity recorded in the Context Broker
#        - error: boolean specifying if there have been any errors throughout the function (associated with requests to the Context Broker or external instances)
def check_num_of_missing_entities(input):
	data_entity_exists, data_entity, error = context_broker_api.get_entity_by_id(input['id'], input['type'])
	if error: return None, None, True
	if not data_entity_exists: return 0, None, False

	quality_entity_exists, quality_entity, error = context_broker_api.get_entity_by_id(input['hasQuality']['object'], "DataQualityAssessment")
	if error: return None, None, True
	if not quality_entity_exists: return 0, None, False

	input_timestamp = basic_operations.get_timestamp(input['dateModified']['value'])
	data_timestamp = basic_operations.get_timestamp(data_entity['dateModified']['value'])

	input_timeliness = round((input_timestamp - data_timestamp)/60, 2)
	data_timeliness = quality_entity['timeliness']['value']

	if input_timeliness > 2.5*data_timeliness: # at least skipped one entity
		num_missing = input_timeliness/data_timeliness -1
	else:
		num_missing = 0

	return round(num_missing), data_timeliness, False


# tagging: create the DataQualityAssessment entity
#     Params: 
#        - attributes: dictionary containing the attributes that have been assessed
#        - id: qaulity entity id
#        - date: date of the assessment
#     Return: 
#        - quality: body of the quality entity generated
def tagging(attributes, id, date):
	# Tagging common body
	quality = {
		"id": id,
		"type": "DataQualityAssessment",
		"dateCalculated": {
			"type": "Property",
			"value": date
		},
		"source": {
			"type": "Property",
			"value": "https://salted-project.eu"
		},
		"@context": [
			"https://raw.githubusercontent.com/SALTED-Project/contexts/main/wrapped_contexts/dataqualityassessment-context.jsonld"
		]
	}
	
	# Tagging specific attributes assessed
	if "accuracy" in attributes:
		quality["accuracy"] = {
			"type": "Property",
			"value": attributes['accuracy'],
			"observedAt": date,
			"unitCode": "CEL"
		}

	if "precision" in attributes:
		quality["precision"] = {
			"type": "Property",
			"value": attributes['precision'],
			"observedAt": date,
			"unitCode": "CEL"
		}

	if "timeliness" in attributes:
		quality["timeliness"] = {
			"type": "Property",
			"value": attributes['timeliness'],
			"observedAt": date,
			"unitCode": "minutes"
		}

	if "completeness" in attributes:
		quality["completeness"] = {
			"type": "Property",
			"value": attributes['completeness'],
			"observedAt": date,
			"unitCode": "P1"
		}

	if "outlier" in attributes:
		quality["outlier"] = {
			"type": "Property",
			"value": {
				"isOutlier": {
					"type": "Property",
					"value": str(attributes['outlier']['boolean'])
				},
				"methodology": {
					"type": "Property",
					"value": attributes['outlier']['info']
				}
				# "methodology": {
				#   "type": "Relationship",
				#   "object": ml_outlier_info
				# }
			},
			"observedAt": date
		}
		
	if "synthetic" in attributes:
		quality["synthetic"] = {
			"type": "Property",
			"value": {
				"isSynthetic": {
					"type": "Property",
					"value": str(attributes['synthetic']['boolean'])
			},
			"methodology": {
				"type": "Property",
				"value": attributes['synthetic']['info']
			}
			# "methodology": {
			#     "type": "Relationship",
			#     "object": ml_synthetic_info
			#   }
			},
			"observedAt": date
		}
	

	return quality