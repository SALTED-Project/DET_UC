# Software Name: generalEnricher.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import pytz, inspect, os, configparser,logging
from datetime import datetime
from geopy.geocoders import Nominatim

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
LOG_LEVEL = config.getint('enricher','LOG_LEVEL')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
handler.setLevel(LOG_LEVEL)
logger.setLevel(LOG_LEVEL)
logger.addHandler(handler)

geolocator = Nominatim(user_agent="SALTED_UC")

def findk(k,tofind):
	if k in tofind: return k,True
	for key in tofind:
		if key.endswith('/'+k): return key,True
	return None,False

def enrich(medida):
	medida_tosend = dict()
	medida_tosend["id"] = medida["id"]
	medida_tosend["type"] = medida["type"].split("#")[-1].split("/")[-1]
	_,flag = findk("address",medida)
	if(flag): return medida_tosend
	loc,flag = findk("location",medida)
	if(not flag): return medida_tosend

	try:
		# request address with given location. cover all possible cases
		coords_pre = medida[loc]["value"]["coordinates"]
		coords_type = medida[loc]["value"]["type"]
		if coords_type == "Point": coords = str(coords_pre[1]) + ", " + str(coords_pre[0])
		elif coords_type == "MultiPoint" or coords_type == "LineString": coords = str(coords_pre[0][1]) + ", " + str(coords_pre[0][0])
		elif coords_type == "MultiLineString" or coords_type == "Polygon": coords = str(coords_pre[0][0][1]) + ", " + str(coords_pre[0][0][0])
		elif coords_type == "MultiPolygon": coords = str(coords_pre[0][0][0][1]) + ", " + str(coords_pre[0][0][0][0])
		else: return medida
		addr = geolocator.reverse(coords)
		address = addr.raw['address']
	except:
		logger.warning("Exception when performing geolocation")
		return medida_tosend

	dtime = datetime.now().astimezone(pytz.utc)
	obsat = str(dtime).replace(' ','T').replace('+00:00','Z')

	# print address as areaServed and more detailed info as address
	medida_tosend["areaServed"] = {"type": "Property", "value": addr.address, "observedAt": obsat}
	writeaddr = dict()
	if "country" in address: writeaddr["addressCountry"] = address["country"]
	if "city" in address: writeaddr["addressLocality"] = address["city"]
	if "state" in address: writeaddr["addressRegion"] = address["state"]
	if "postcode" in address: writeaddr["postalCode"] = address["postcode"]
	if "road" in address: writeaddr["streetAddress"] = address["road"]
	medida_tosend["address"] = {"type": "Property", "value": writeaddr, "observedAt": obsat}

	return medida_tosend

if __name__ == '__main__':
	medida = {
	"id": "urn:ngsi-ld:BikeHireDockingStation:santander:1",
	"location": {
		"type": "GeoProperty",
		"value": {
			"type": "Point",
			"coordinates": [
				-3.8021,
				43.4617
			]
		},
		"observedAt": "2022-05-30T14:55:01Z"
	}
	}
	medida_enriched = enrich(medida)
	#print(json.dumps(medida_enriched, indent=3))
