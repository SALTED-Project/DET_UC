# Software Name: anomaly_detection.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import json
from dateutil import parser
import numpy as np
import pandas as pd
from dateutil import relativedelta
from src_quality_assessment import detectors


# Building detectors
training_filename = "src_quality_assessment/files/training_data.csv"
data = pd.read_csv(training_filename, delimiter=";")
data['date'] = pd.to_datetime(data['date'], format = "ISO8601")

# StreamingExponentialMovingAverage_UC_modified
threshold = 3; min_no_records = 96; alpha = 0.45
parameters = {'threshold': threshold, 'min_no_records': min_no_records,'alpha':alpha}
model_expmovavg = detectors.StreamingExponentialMovingAverage_UC_modified(**parameters)
model_expmovavg.train(data)
print("anomaly_detection.py", flush=True)

# Possible next synthetic value 
possible_synthetic_value = 0


def UC_exponentialmovingaverage(input):
  d = pd.to_datetime(input['value']['observedAt'], format = "ISO8601")
  v = input['value']['value']

  pred = model_expmovavg.detect(d, v, dumping=False)
  info = " MODIFIED CODE: https://www.kaggle.com/code/leomauro/seasonal-anomaly-detection-streaming-data?scriptVersionId=93383598 "

  model_expmovavg.update(d, v, pred, training_filename)

  return pred, info


def novelty_detection(input, is_synthetic, list_of_processes):
  outlier = {}
  if 'outlier' in list_of_processes and not is_synthetic:
    is_outlier, info = UC_exponentialmovingaverage(input)
    outlier = {'outlier': {'boolean': is_outlier, 'info': info}}
  else:
    is_outlier = False; info = "N/A"
    outlier = {'outlier': {'boolean': is_outlier, 'info': info}}
  
  return outlier


def create_synthetic_value(input, is_synthetic, list_of_processes):
  synthetic = {}
  if 'synthetic' in list_of_processes and is_synthetic:
    d = pd.to_datetime(input['value']['observedAt'], format = "ISO8601")
    possible_synthetic_value = model_expmovavg._expected_value(d)
    info = " MODIFIED CODE: https://www.kaggle.com/code/leomauro/seasonal-anomaly-detection-streaming-data?scriptVersionId=93383598 "
    synthetic = {'synthetic':{'boolean': is_synthetic, 'value': possible_synthetic_value, 'info': info}}
  else:
    possible_synthetic_value = None ; info = "N/A"
    synthetic = {'synthetic':{'boolean': is_synthetic, 'value': possible_synthetic_value, 'info': info}}

  return synthetic