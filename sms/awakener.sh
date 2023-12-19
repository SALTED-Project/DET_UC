#!/bin/bash

# Software Name: awakener.sh
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

#stop gracefully
_term() {
  ps -ef | grep sms | grep -v grep | awk '{print $2}' | xargs kill 2 &> /dev/null
  sleep 1 &
  wait $!
  exit 0
}
trap _term SIGTERM

#variable with the last log time
last_time=0
python -u sms.py &

while true
do
    #sleep 5 min and check if program is stuck
    sleep 300 &
    wait $!
    current_time=$(tail -n 1 logs/sms.log | cut -d' ' -f1,2)

    if [ $(date -d "$last_time" +%s) -ne $(date -d "$current_time" +%s) ]; then
        last_time=$current_time
    else
        ps -ef | grep sms | grep -v grep | awk '{print $2}' | xargs kill 2 &> /dev/null
        echo "Program idle detected. Reviving in 5 seconds."
        sleep 5 &
        wait $!
        python -u sms.py &
    fi
done
