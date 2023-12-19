# Software Name: specificEnricher.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import pytz
from datetime import datetime

# Temperatura: cold, warm, hot (10.3390/ijerph15061112)
# SoundPressure: quiet, average, loud (10.1177/000348944705600310)
# Traffic: fluid, average, dense, congested
# AirQuality: safe, average, high, dangerous (https://apps.who.int/iris/handle/10665/345329)
# PM25:	5, 15, 35, +
# PM10:	15, 30, 70, +
# O3:	60, 70, 100, +
# NO2:	10, 25, 40, +
# SO2:	40, 75, 125, +
# CO:	3000, 4000, 7000, +
AIRQLEVELS = {
	"pm25": [5,15,35],
	"pm10": [15,30,70],
	"o3": [60,70,100],
	"no2": [10,25,40],
	"so2": [40,75,125],
	"co": [3000,4000,7000]
}

def findk(k,tofind):
	if k in tofind: return k,True
	for key in tofind:
		if key.endswith('/'+k): return key,True
	return None,False

def enrich(medida,medida_tosend):
	dtime = datetime.now().astimezone(pytz.utc)
	obsat = str(dtime).replace(' ','T').replace('+00:00','Z')
	tipo = medida["type"].split("#")[-1].split("/")[-1]

	if tipo == "AirQualityObserved":
		for param in ["pm25","pm10","o3","no2","so2","co"]: # list of properties to check
			kparam,flag = findk(param,medida)
			if not flag: continue
   
			# extract value and unit
			try:
				value = float(medida[kparam]["value"])
				kunit,flag = findk("unitCode",medida[kparam])
				if flag: uc = medida[kparam][kunit]
				else: uc = "GQ" if value > 1.0 else "GP"
				if uc == "GP": value *= 1000
			except: continue
			
			# assess value
			if value < AIRQLEVELS[param][0]: descr = "Safe"
			elif value < AIRQLEVELS[param][1]: descr = "Average"
			elif value < AIRQLEVELS[param][2]: descr = "High"
			else: descr = "Dangerous"
	
			# update description if it has changed
			k2,flag = findk("concentrationLevel",medida[kparam])
			source = {"type": "Property", "value":"https://apps.who.int/iris/handle/10665/345329"}
			if flag:
				if (not (medida[kparam][k2]["value"] == descr)):
					medida_tosend[kparam] = medida[kparam]
					medida_tosend[kparam]["concentrationLevel"] = {"type": "Property", "value": descr, "observedAt": obsat, "source": source}
			else:
				medida_tosend[kparam] = medida[kparam]
				medida_tosend[kparam]["concentrationLevel"] = {"type": "Property", "value": descr, "observedAt": obsat, "source": source}

	elif tipo == "SoundPressureLevel":
		key,flag = findk("sounddB",medida)
		if not flag: return medida_tosend
  
		# extract value
		try: val = float(medida[key]["value"])
		except: return medida_tosend
		
		# assess value
		if val < 45: descr = "Quiet"
		elif val < 60: descr = "Average"
		elif val < 80: descr = "Loud"
		else: descr = "Harmful"
  
		# update description if it has changed
		k2,flag = findk("perception",medida[key])
		source = {"type": "Property", "value":"10.1177/000348944705600310"}
		if flag:
			if (not (medida[key][k2]["value"] == descr)):
				medida_tosend[key] = medida[key]
				medida_tosend[key]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat, "source": source}
		else:
			medida_tosend[key] = medida[key]
			medida_tosend[key]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat, "source": source}  
		
	elif tipo == "Temperature":
		# extract value and unit
		key,flag = findk("value",medida)
		if not flag: return medida_tosend
		kunit,flagunit = findk("unit",medida)
		try:
			val = float(medida[key]["value"])
			kuc,flaguc = findk("unitCode",medida[key])
			if flaguc: uc = medida[key][kuc]
			elif flagunit: uc = medida[kunit]["value"]
			else: uc = "CEL"
		except: return medida_tosend
		
		# assess value
		if uc == "CEL":
			if val < 1: descr = "Very cold"
			elif val < 11: descr = "Cold"
			elif val < 20: descr = "Cool"
			elif val < 26: descr = "Warm"
			elif val < 35: descr = "Hot"
			elif val < 45: descr = "Very hot"
			else: descr = "Harmful"
		elif uc == "FAH":
			if val < 32: descr = "Very cold"
			elif val < 51: descr = "Cold"
			elif val < 68: descr = "Cool"
			elif val < 78: descr = "Warm"
			elif val < 95: descr = "Hot"
			elif val < 112: descr = "Very hot"
			else: descr = "Harmful"
		else: return medida_tosend
	
		# update description if it has changed
		k2,flag = findk("perception",medida[key]) 
		source = {"type": "Property", "value":"10.3390/ijerph15061112"}
		if flag:
			if (not (medida[key][k2]["value"] == descr)):
				medida_tosend[key] = medida[key]
				medida_tosend[key]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat}
		else:
			medida_tosend[key] = medida[key]
			medida_tosend[key]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat}  

	elif tipo == "TrafficFlowObserved":
		# extract value
		kocc,flag = findk("occupancy",medida)
		if not flag: return medida_tosend
		kcong,flag = findk("congested",medida)
		try:
			if flag:
				congested = medida[kcong]["value"]
				if congested:
					# update description if it has changed
					k2,flag = findk("perception",medida[kocc])
					if flag:
						if (not (medida[kocc][k2]["value"] == descr)):
							medida_tosend[kocc] = medida[kocc]
							medida_tosend[kocc]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat}
					else:
						medida_tosend[kocc] = medida[kocc]
						medida_tosend[kocc]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat}  

					return medida_tosend
   
			occupancy = float(medida[kocc]["value"])
			kuc,flaguc = findk("unitCode",medida[kocc])
			if flaguc: uc = medida[kocc][kuc]
			if uc == "P1": occupancy /= 100

		except: return medida_tosend
		
		# assess value
		if occupancy < 0.2: descr = "Fluid"
		elif occupancy < 0.5: descr = "Average"
		elif occupancy < 0.8: descr = "Dense"
		else: descr = "Congested"
	
		# update description if it has changed
		k2,flag = findk("perception",medida[key])
		if flag:
			if (not (medida[key][k2]["value"] == descr)):
				medida_tosend[key] = medida[key]
				medida_tosend[key]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat}
		else:
			medida_tosend[key] = medida[key]
			medida_tosend[key]["perception"] = {"type": "Property", "value": descr, "observedAt": obsat}

	return medida_tosend

if __name__ == '__main__':
	medida = {
        "id": "urn:ngsi-ld:AirQualityObserved:smartsantander:u7jcfa:f3032",
        "type": "AirQualityObserved",
        "https://smartdatamodels.org/dataModel.Environment/co": {
            "type": "Property",
            "value": 0.1,
            "observedAt": "2022-08-01T10:43:12Z",
            "unitCode": "GP"
        },
        "https://smartdatamodels.org/dataModel.Environment/no2": {
            "type": "Property",
            "value": 23.0,
            "observedAt": "2022-08-01T10:43:12Z",
            "unitCode": "GQ"
        },
        "https://smartdatamodels.org/dataModel.Environment/o3": {
            "type": "Property",
            "value": 12.0,
            "observedAt": "2022-08-01T10:43:12Z",
            "unitCode": "GQ"
        },
        "https://smartdatamodels.org/dataModel.Environment/relativeHumidity": {
            "type": "Property",
            "value": 64.0,
            "observedAt": "2022-08-01T10:43:12Z",
            "unitCode": "P1"
        },
        "location": {
            "type": "GeoProperty",
            "value": {
                "type": "Point",
                "coordinates": [
                    -3.83153,
                    43.457
                ]
            },
            "observedAt": "2022-08-01T10:43:12Z"
        }
    }
	medida_enriched = enrich(medida)
	#print(json.dumps(medida_enriched, indent=3))
