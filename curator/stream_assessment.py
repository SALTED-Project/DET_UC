# Software Name: stream_assessment.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import copy
from dateutil.relativedelta import relativedelta
from src_quality_assessment import context_broker_api, basic_operations, quality_assessment
import injector

# main: curation process
#     Params: 
#        - input: input entity being assessed
def main(input, http_session):

	# Add the relationship with the dataQualityAssessment entity
	quality_id = "urn:ngsi-ld:DataQualityAssessment:"+input['id'][12:]
	input['hasQuality'] = {
		"type": "Relationship",
		"object": quality_id,
	}

	# Missing values 
	num_missing_entities = 0 
	if input["type"] == "Temperature" and basic_operations.check_coordinates(input['location']['value']['coordinates'][0], input['location']['value']['coordinates'][1]): 
		# Inside Santander
		num_missing_entities, data_timeliness, error = quality_assessment.check_num_of_missing_entities(input)
		if error: return None, True

	if num_missing_entities >= 10: # too many missing entities --> act like there wasn't a previous quality entity (start over)
		quality_input, _, error = quality_assessment.do_quality_assessment(input, False)
		if error: return None, True

		# Penalize completeness metric
		quality_input["completeness"]["value"] = 0

		return [input, quality_input], False

	input_date = (
		basic_operations.get_date(input['dateObserved']['value'])
		if "dateObserved" in input
		else basic_operations.get_date(input['dateModified']['value']) 
	)
	while num_missing_entities < 10 and num_missing_entities > 0:
		synthetic_input = copy.deepcopy(input)
		
		# Change/set synthetic timestamp
		aux_minutes = num_missing_entities * data_timeliness
		synthetic_date = (input_date - relativedelta(minutes=aux_minutes)).strftime('%Y-%m-%dT%H:%M:%SZ')
		if "dateObserved" in synthetic_input:
			synthetic_input['dateObserved']['value'] = synthetic_date
		else:
			synthetic_input['dateModified']['value'] = synthetic_date

		synthetic_input['value']['observedAt'] = synthetic_date
		if synthetic_input['id'].find(":f")!=-1: synthetic_input['location']['observedAt'] = synthetic_date

		quality_input, synthetic_input['value']['value'], error = quality_assessment.do_quality_assessment(synthetic_input, True)
		if error: return None, True

		# Created synthetic_input entity and quality entity -- Update in the Context Broker
		sc = injector.inject([synthetic_input, quality_input], http_session)

		num_missing_entities = num_missing_entities - 1

	# Input value -- Update in the Context Broker
	quality_input, _, error = quality_assessment.do_quality_assessment(input, False)
	if error: return None, True

	return [input, quality_input], False


if __name__ == '__main__':
	medida = '''{
    "id": "urn:TrafficFlowObserved:testV",
    "type": "TrafficFlowObserved",
    "occupancy": {
        "type": "Property",
        "value": 226,
        "observedAt": "2022-05-20T07:42:45Z"
    },
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [
                -3.4391538,
                43.263564
            ]
        }
    },
    "@context": [
      "https://smartdatamodels.org/context.jsonld",
      "https://raw.githubusercontent.com/smart-data-models/dataModel.Transportation/master/context.jsonld"
   ]
}'''
	medida_curated = main(medida)
	#print(medida,"\n----------\n",medida_curated)

