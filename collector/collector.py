# Software Name: collector.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import time, threading, os, configparser, signal
import barcelona, barcelona_bicis, bilbao, murcia_aq, santander_bicis, santander_buses, valencia, vitoria, s_config
from control_loop import ControlLoopHandler
s_config.exit_event.clear()

# Get variables from config file
PROGRAM_PATH = os.path.dirname(os.path.realpath(__file__))
config = configparser.ConfigParser()
config.read(PROGRAM_PATH + '/general.conf')
starting_value = config.getboolean('params','REDUCED_FREQ')
det_id = "uc_collector"
det_clh = ControlLoopHandler(det_id,{"reduced_frequency":starting_value})
det_clh.start()

def exit_gracefully(signal, _):
	s_config.exit_event.set()
	det_clh.stop()
	main_th = threading.current_thread()
	for th in threading.enumerate():
		if th is main_th: continue
		th.join()
	print("Gracefully stopped")
	exit(0)

signal.signal(signal.SIGINT, exit_gracefully)
signal.signal(signal.SIGTERM, exit_gracefully)

i_min = 0
while True: # run every minute
	sleeptime = 60.0 - (time.time() % 60.0)
	time.sleep(sleeptime)
	reduced_freq = det_clh.get_param("reduced_frequency")
	if (type(reduced_freq) != bool): # recheck frequency of requests
		try: reduced_freq = bool(reduced_freq)
		except: reduced_freq = starting_value
	short_freq = 60 if reduced_freq else 5
	med_freq = 60 if reduced_freq else 10
	long_freq = 60 if reduced_freq else 15

	# request based on availability of data source
	if (i_min % short_freq) == 0:
		th_bilbao = threading.Thread(target=bilbao.collect)
		th_bilbao.start()

	if ((i_min-1) % short_freq) == 0:
		th_barcelonab = threading.Thread(target=barcelona_bicis.collect)
		th_barcelonab.start()
  
	if ((i_min-2) % short_freq) == 0:
		th_santanderbu = threading.Thread(target=santander_buses.collect)
		th_santanderbu.start()

	if ((i_min-3) % med_freq) == 0:
		th_santanderb = threading.Thread(target=santander_bicis.collect)
		th_santanderb.start()

	if ((i_min-4) % long_freq) == 0:
		th_barcelona = threading.Thread(target=barcelona.collect)
		th_barcelona.start()

	if ((i_min-9) % long_freq) == 0:
		th_valencia = threading.Thread(target=valencia.collect)
		th_valencia.start()

	if ((i_min-14) % long_freq) == 0:
		th_vitoria = threading.Thread(target=vitoria.collect)
		th_vitoria.start()

	if ((i_min-8) % 60) == 0:
		th_murciaaq = threading.Thread(target=murcia_aq.collect)
		th_murciaaq.start()

	# reset every day
	i_min += 1
	if i_min >= 1440:
		i_min = 0
