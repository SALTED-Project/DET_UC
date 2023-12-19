# Software Name: detectors.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Laura MARTIN <lmartin@tlmat.unican.es> et al.

import pickle
import pandas as pd
import numpy as np
import datetime as dt

# Seasonal Anomaly Detection - Streaming Data (https://www.kaggle.com/code/leomauro/seasonal-anomaly-detection-streaming-data?scriptVersionId=93383598)
# Code modified
class StreamingMovingAverage_UC_modified:
	'''Moving Average algorithm'''
	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.rolling.html

	def __init__(self, threshold=1.0, min_no_records = 3) -> None:
		# Parameters
		self.max_deviation_from_expected = threshold
		self.min_nof_records_in_model = min_no_records
		self.max_nof_records_in_model = 3 * self.min_nof_records_in_model

	def detect(self, timestamp: int, value: float, dumping: bool=False) -> bool:
		'''Detect if is a Anomaly'''
		self._update_state(timestamp, value)
		expected_value = self._expected_value(timestamp)
		# is there enough data and is not NaN value
		response, curr_value, deviation = False, value, 0.0
		if self._enough_data() and not np.isnan(expected_value):
			# is the value out of the boundary? when it decrease
			curr_value = expected_value
			deviation = self._standard_deviation() * self.max_deviation_from_expected
			# when it is higher than expected
			if expected_value + deviation < value or expected_value - deviation > value:
					response = True
		# dumping or not
		if dumping: return (response, curr_value, deviation)
		else: return response

	def _update_state(self, timestamp: int, value: float) -> None:
		'''Update the model state'''
		# check if it is the first time the model is run or if there is a big interval between the timestamps
		if not hasattr(self, 'previous_timestamp'):
			self._init_state(timestamp)
		# is there a lot of data? remove one record
		if len(self.data_streaming) > self.max_nof_records_in_model:
			self.data_streaming.pop(0)

	def _init_state(self, timestamp: int) -> None:
		'''Reset the parameters'''
		self.previous_timestamp = timestamp
		self.data_streaming = list()

	def _enough_data(self) -> bool:
		'''Check if there is enough data'''
		return len(self.data_streaming) >= self.min_nof_records_in_model

	def _expected_value(self, timestamp: int) -> float:
		'''Return the expected value'''
		data = self.data_streaming
		data = pd.Series(data=data, dtype=float)
		many = self.min_nof_records_in_model
		return data.rolling(many, min_periods=1).mean().iloc[-1]

	def _standard_deviation(self) -> float:
		'''Return the standard deviation'''
		data = self.data_streaming
		return np.std(data, axis=0)

	def get_state(self) -> dict:
		'''Get the state'''
		self_dict = {key: value for key, value in self.__dict__.items()}
		return pickle.dumps(self_dict, 4)

	def set_state(self, state) -> None:
		'''Set the state'''
		_self = self
		ad = pickle.loads(state)
		for key, value in ad.items():
			setattr(_self, key, value)

class StreamingExponentialMovingAverage_UC_modified(StreamingMovingAverage_UC_modified):
	'''Exponential Weighted Moving Average (EWMA) algorithm'''
	# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.Series.ewm.html

	def __init__(self, threshold=1.0, min_no_records = 3, alpha=0.3) -> None:
		super().__init__(threshold, min_no_records)
		# Parameters
		self.alpha = alpha
		self.data_queue = list()

	def _enough_data(self) -> bool:
		'''Check if there is enough data'''
		return len(self.data_streaming) > 0

	def _expected_value(self, timestamp: int) -> float:
		'''Return the expected value'''
		data = self.data_streaming
		data = pd.Series(data=data, dtype=float)
		return data.ewm(alpha=self.alpha, adjust=True).mean().iloc[-1]

	def train(self, df: pd.core.frame.DataFrame) -> None:
		if not hasattr(self, 'previous_timestamp'):
			self._init_state(df['date'][0])
		for index, row in df.iterrows():
			self.previous_timestamp = row["date"]
			self.data_streaming.append(row["value"])
																							
	def update(self, d: dt.date, value: float, pred: bool, file: str) -> None:
		if not pred: # is not an anomaly
			self.data_queue.append(value)
		
		if (d - self.previous_timestamp) > dt.timedelta(minutes=5):
			self.previous_timestamp = self.previous_timestamp + dt.timedelta(minutes=5)
			new_value = sum(self.data_queue) / len(self.data_queue)
			self.data_streaming.append(new_value)
			self.data_queue.clear() 

			new_csv_data = pd.DataFrame(); new_csv_data['date'] = [self.previous_timestamp]; new_csv_data['value'] = [new_value]
			with open(file, 'a') as f:
				new_csv_data.to_csv(f, header=False, sep=";", index = False)
		
