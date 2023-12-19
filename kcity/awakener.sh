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
  ps -ef | grep kcity | grep -v grep | awk '{print $2}' | xargs kill &> /dev/null
  exit 0
}
trap _term SIGTERM

#variable with the last log time
last_time=0
python -u kcity_ttn_salted.py &

while true
do
    #sleep 10 min and check if it has changed
    sleep 600 &
    wait $!
    current_time=$(tail -n 1 logs/kcity.log | cut -d' ' -f1,2)

    if [ $(date -d "$last_time" +%s) -ne $(date -d "$current_time" +%s) ]; then
        last_time=$current_time
    else
        #if not, restart program
        ps -ef | grep kcity | grep -v grep | awk '{print $2}' | xargs kill
        echo "Program idle detected. Reviving in 10 seconds."
        sleep 10 &
        wait $!
        python -u kcity_ttn_salted.py &
    fi
done
