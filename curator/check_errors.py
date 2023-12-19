# Software Name: check_errors.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import pytz
from datetime import datetime

def findk(k,tofind):
	if k in tofind: return k,True
	for key in tofind:
		if key.endswith('/'+k): return key,True
	return None,False

def check(medida,tipo):
	error = 0
	dtime = datetime.now().astimezone(pytz.utc)
	obsat = str(dtime).replace(' ','T').replace('+00:00','Z')
	#check coordinates are not 0,0
	key,flag = findk("location",medida)
	if flag:
		if (medida[key]["value"]["coordinates"] == [0,0]) or (medida[key]["value"]["coordinates"] == ["0","0"]):
			del medida["location"]
			error = 1
			medida["location_unavailable"] = {"type":"Property","value":True, "observedAt": obsat}
	else:
		error = 1
		medida["location_unavailable"] = {"type":"Property","value":True, "observedAt": obsat}

	#check valid airquality values
	if tipo == "AirQualityObserved":
		key,flag = findk("relativeHumidity",medida)
		if flag:
			if int(medida[key]["value"]) > 100:
				error = 1
				medida["faulty_data"] = {"type":"Property","value":"relativeHumidity out of range", "observedAt": obsat}

	#check valid traffic values
	if tipo == "TrafficFlowObserved":
		key,flag = findk("occupancy",medida)
		if flag:
			if int(medida[key]["value"]) > 100:
				error = 1
				medida["faulty_data"] = {"type":"Property","value":"occupancy out of range", "observedAt": obsat}
		key,flag = findk("intensity",medida)
		if flag:
			if int(medida[key]["value"]) < 0:
				error = 1
				medida["faulty_data"] = {"type":"Property","value":"intensity out of range", "observedAt": obsat}
		key,flag = findk("averageVehicleSpeed",medida)
		if flag:
			if int(medida[key]["value"]) < 0:
				error = 1
				medida["faulty_data"] = {"type":"Property","value":"averageVehicleSpeed out of range", "observedAt": obsat}

	if not error:
		key,flag = findk("faulty_data",medida)
		if(flag): medida[key] = {"type":"Property","value":False, "observedAt": obsat}
		key,flag = findk("location_unavailable",medida)
		if(flag): medida[key] = {"type":"Property","value":False, "observedAt": obsat}
	return medida,error

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
	medida, error = check(medida, "TrafficFlowObserved")
	#if(error): print("La medida tiene errores")
 