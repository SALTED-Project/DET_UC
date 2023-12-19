# Software Name: s_jmespath.py
# SPDX-FileCopyrightText: Copyright (c) 2023 Universidad de Cantabria
# SPDX-License-Identifier: LGPL-3.0 
#
# This software is distributed under the LGPL-3.0 license;
# see the LICENSE file for more details.
#
# Author: Victor GONZALEZ (Universidad de Cantabria) <vgonzalez@tlmat.unican.es> et al.

import s_config, copy
from jmespath import functions

class SaltedFunctions(functions.Functions):
    
    def __init__(self):
        self.medida = copy.deepcopy(s_config.medida)

    # from a ISO8601 formatted datetime, get its timestamp
    @functions.signature({'types': ['string']})
    def _func_get_timestamp(self, s):
        from dateutil import parser
        from datetime import datetime
        dateobj = parser.parse(s)
        timest = datetime.timestamp(dateobj)
        return int(timest)

    # create the id parameter
    @functions.signature({'types': ['string']})
    def _func_get_id(self, t):
        if "id" in self.medida:
            return "urn:ngsi-ld:" +  t + ":" + str(self.medida["id"])
        else:
            import uuid
            nueva_id = str(uuid.uuid4())
            return "urn:ngsi-ld:" +  t + ":" + nueva_id

    @functions.signature()
    def _func_get_date(self):
        if "dateObserved" in self.medida:
            return self.medida["dateObserved"]
        elif "dateModified" in self.medida:
            return self.medida["dateModified"]
        else:
            from datetime import datetime
            import pytz
            dt = datetime.now()
            d_utc = dt.astimezone(pytz.utc)
            newdate = str(d_utc).replace(' ','T')
            newdate = newdate.replace('+00:00','Z')
            return newdate
        