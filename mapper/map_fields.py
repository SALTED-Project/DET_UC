# Software Name: map_fields.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import re, string, numpy, json, s_config, copy, jmespath, os, inspect, configparser, logging
from prettytable import PrettyTable
from s_jmespath import SaltedFunctions
from sklearn.feature_extraction.text import CountVectorizer

PROGRAM_NAME = inspect.stack()[0][1].split('.py', 1)[0].split('\\')[-1].split('/')[-1]
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))

# Get variables from config file
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
LOG_LEVEL = config.getint('mapper','LOG_LEVEL')

# Set up logger
logger = logging.getLogger(PROGRAM_NAME + "_logger")
formatter = logging.Formatter('{asctime} {levelname:<8s} | {filename}:{lineno:<4} [{funcName:^30s}] | {message}', style='{')
handler = logging.StreamHandler()
handler.setFormatter(formatter)
handler.setLevel(LOG_LEVEL)
logger.setLevel(LOG_LEVEL)
logger.addHandler(handler)

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                if (a != "location") and (a != "geometry"):
                  flatten(x[a], name + a + '_')
                else:
                  out[name[:-1] + a] = x[a]
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def json_to_text(input_data):
  data_str = json.dumps(input_data)
  lowercase = data_str.casefold()
  #only_text = re.sub('[0-9]+', "", lowercase)
  only_text = re.sub('(?<!co|pm|m1|m2|no|so)[0-9]+', "", lowercase)
  spaces = re.sub(f"[{re.escape(string.punctuation)}]", " ", only_text)
  final = re.sub(' +', " ", spaces)
  return final

def write_new_words(medida,origen,tipo):
  # check for new words
  wordList = [field.split() for field in medida]
  wordList = sum(wordList, [])
  wordList = list(dict.fromkeys(wordList))
  wordOrigen = "".join(origen)
  try:
    with open(PROGRAM_PATH + "/sdm/"+tipo+"/words.txt", "r") as file: pendingWords = file.read()
  except:
    pendingWords = ""

  for word in wordList:
    if word not in wordOrigen:
      if word not in pendingWords:
        with open(PROGRAM_PATH + "/sdm/"+tipo+"/words.txt", "a") as file: file.write(word+"\n")

  return

def get_origen_template(tipo):

    # get correct template
    if (tipo=="Temperature"):
        template = 'temperature.template.jmespath'
        origen = 'Temperature/temperature.txt'
    elif (tipo=="BatteryStatus"):
        template = 'battery.template.jmespath'
        origen = 'BatteryStatus/battery.txt'
    elif (tipo=="AirQualityObserved"):
        template = 'airquality.template.jmespath'
        origen = 'AirQualityObserved/airquality.txt'
    elif (tipo=="SoundPressureLevel"):
        template = 'spressure.template.jmespath'
        origen = 'SoundPressureLevel/spressure.txt'
    elif (tipo=="ParkingSpot"):
        template = 'parking.template.jmespath'
        origen = 'ParkingSpot/parking.txt'
    elif (tipo=="ElectroMagneticObserved"):
        template = 'electromagnetic.template.jmespath'
        origen = 'ElectroMagneticObserved/electromagnetic.txt'
    elif (tipo=="BikeHireDockingStation"):
        template = 'bikehire.template.jmespath'
        origen = 'BikeHireDockingStation/bikehire.txt'
    elif (tipo=="TrafficFlowObserved"):
        template = 'trafficflow.template.jmespath'
        origen = 'TrafficFlowObserved/trafficflow.txt'
    elif (tipo=="FleetVehicleStatus"):
        template = 'fleet.template.jmespath'
        origen = 'FleetVehicleStatus/fleet.txt'
    else:
        logger.error("Type "+tipo+" unknown")
        exit()

    with open(PROGRAM_PATH + "/templates/"+template, "r") as file: f_template = file.read()
    with open(PROGRAM_PATH + "/sdm/"+origen, "r") as file: f_origen = json.loads(file.read())
    return f_origen, f_template

def mapper(medida, tipo, unitData):
  origen, template = get_origen_template(tipo)

  origen_fields = [{k:v} for k,v in origen.items()]
  origen_text = [json_to_text(field) for field in origen_fields]

  medida_flattened = flatten_json(medida)
  medida_fields = [{k:v} for k,v in medida_flattened.items()]
  medida_text = [json_to_text(field) for field in medida_fields]

  write_new_words(medida_text,origen_text,tipo) #save new words
  origen_text.pop() #remove the "nocategory" words for computation

  # create the transform
  vectorizer = CountVectorizer(token_pattern=r"(?u)\b\w+\b")
  # tokenize and build vocab
  vectorizer.fit(origen_text)

  origen_vec = vectorizer.transform(origen_text).toarray()
  train_vecs = list()
  for vec_field in origen_vec:
    train_vecs.append(list(numpy.where(vec_field == 1)[0]))

  medida_vec = vectorizer.transform(medida_text).toarray()
  test_vecs = list()
  for vec_field in medida_vec:
    test_vecs.append(list(numpy.where(vec_field == 1)[0]))

  train_labels = [k for k,v in origen.items()]
  numTrainVecs = len(train_vecs)
  numTestVecs = len(test_vecs)

  final_keys = numpy.array([])
  distancia = list()
  cuentas = list()
  verosimilitud = list()
  nombres = ["Baja","Media","Alta","Muy alta"]
  for it_test in range(0,numTestVecs):
    arrayNewDist = [numpy.array([]),numpy.array([])]
    
    for it_train in range(numTrainVecs):
      min_dist = 1000 #"infinite"
      count = 0
      for elem in train_vecs[it_train]:
        for elem2 in test_vecs[it_test]:
          dist = abs(elem - elem2)
          if dist < min_dist:
            min_dist = dist
            count = 1
          elif dist == min_dist:
            count += 1

      arrayNewDist[0] = numpy.append(arrayNewDist[0], min_dist)
      arrayNewDist[1] = numpy.append(arrayNewDist[1], count)

    distIndex = numpy.argsort(arrayNewDist[0]) # sorting distance and returning indices that achieves sort
    dist_ordenada = numpy.take(arrayNewDist[0], distIndex)
    count_ordenada = numpy.take(arrayNewDist[1], distIndex)
    distancia_min = dist_ordenada[0]
    for it_dist in range(len(dist_ordenada)):
      if (dist_ordenada[it_dist] > distancia_min):
        dist_ordenada = dist_ordenada[0:it_dist]
        count_ordenada = count_ordenada[0:it_dist]
        break
    
    countIndex = numpy.argsort(count_ordenada)
    finalIndex = distIndex[countIndex[-1]]
    final_dist = dist_ordenada[countIndex[-1]]
    final_counts = count_ordenada[countIndex[-1]]
    distancia.append(final_dist)
    cuentas.append(final_counts)
    if (final_dist != 0):
      verosimilitud.append(0)
    else:
      if (final_counts == 1):
        verosimilitud.append(1)
      elif (final_counts == 2):
        verosimilitud.append(2)
      else:
        verosimilitud.append(3)

    label = train_labels[finalIndex]
    final_keys = numpy.append(final_keys, label)

  # Print all
  table = PrettyTable(["Campo","Tipo predicho","Distancia","Cuentas","Verosimilitud"])
  for index in range(len(test_vecs)):
    table.add_row([medida_text[index],final_keys[index],distancia[index],cuentas[index],nombres[verosimilitud[index]]])
  logger.debug(table)

  # change the json keys to the predicted ones in likelihood order (3,2,1)
  veroIndex = numpy.argsort(verosimilitud)
  veroIndex = numpy.flip(veroIndex)
  keys = list(medida_flattened.items())
  if isinstance(unitData, dict):
    unitData = flatten_json(unitData)
  medida_mapped = dict()
  medida_mapped["unitDataSalted"] = dict()
  for it in veroIndex:
    if (verosimilitud[it] == 0): break
    newKey = final_keys[it]
    if (newKey in medida_mapped): continue
    medida_mapped[newKey] = medida_flattened[keys[it][0]]
    try:
      medida_mapped["unitDataSalted"][newKey] = unitData[keys[it][0]]
    except:
      continue

  s_config.medida = copy.deepcopy(medida_mapped)

  # use the template
  options = jmespath.Options(custom_functions=SaltedFunctions())
  medida_ngsild = jmespath.search(template, medida_mapped, options=options)

  # remove empty properties
  to_delete = list()
  for k,v in medida_ngsild.items():
    if not hasattr(v, "__getitem__"): continue
    if "value" in v:
      if v["value"] == None:
        to_delete.append(k)
    if "object" in v:
      if v["object"] == None:
        to_delete.append(k)
    if "unitCode" in v:
      if v["unitCode"] == None:
        del medida_ngsild[k]["unitCode"]
  for k in to_delete: del medida_ngsild[k]

  return medida_ngsild


if __name__ == '__main__':
  tipo_test = "TrafficFlowObserved"
  with open(PROGRAM_PATH + "/sdm/" + tipo_test + "/ej.txt", "r") as f:
    medida = json.loads(f.read())

  medida_mapeada = mapper(medida,tipo_test,None)

  logger.debug(json.dumps(medida_mapeada,indent=3))
