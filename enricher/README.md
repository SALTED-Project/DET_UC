# Data Enrichers and Data Linkers by Universidad de Cantabria
This repository contains the source code of the Data Enrichers and Data Linkers developed by Universidad de Cantabria (UC) within the framework of the [SALTED project](https://salted-project.eu/). These components recieve NGSI-LD entities via notifications from a Context Broker and add value to them by adding new properties (in the case of enrichers) or relationships (in the case of linkers). The new high-quality entities are re-injected into the NGSI-LD Context Broker.

The proximity distance surveyed by the Geolocation Data Linker may be reconfigured on the fly using the Control Loop mechanism.

## Acknowledgement
This work was supported by the European Commission CEF Programme by means of the project SALTED ‘‘Situation-Aware Linked heTerogeneous Enriched Data’’ under the Action Number 2020-EU-IA-0274.

## License
This material is licensed under the GNU Lesser General Public License v3.0 whose full text may be found at the *LICENSE* file located in the root of the repository.

It mainly makes use of the following libraries and frameworks (dependencies of dependencies have been omitted):

| Library / Framework |   Licence    |
|---------------------|--------------|
| Flask          | BSD-3-Clause          |
| Flask_RESTful          | BSD-3-Clause          |
| geopy          | MIT          |
| paho_mqtt          | EPL v2 / EDL v1          |
| pytz             | MIT          |
| Requests                 | Apache 2.0          |
| waitress          | ZPL 2.1     |
