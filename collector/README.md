# Batch Data Collector by Universidad de Cantabria
This repository contains the source code of the batch Data Collector developed by Universidad de Cantabria (UC) within the framework of the [SALTED project](https://salted-project.eu/). This component harvests data from heterogeneous sources, namely Open Data Portals, via HTTP requests, and sends the pre-processed data to the Data Mapper. The collection frequency of a running instance of this component may also be reconfigured on the fly via the Control Loop mechanism.

## Acknowledgement
This work was supported by the European Commission CEF Programme by means of the project SALTED ‘‘Situation-Aware Linked heTerogeneous Enriched Data’’ under the Action Number 2020-EU-IA-0274.

## License
This material is licensed under the GNU Lesser General Public License v3.0 whose full text may be found at the *LICENSE* file located in the root of the repository.

It mainly makes use of the following libraries and frameworks (dependencies of dependencies have been omitted):

| Library / Framework |   Licence    |
|---------------------|--------------|
| python_dateutil          | Apache 2.0 and BSD-3-Clause          |
| pytz             | MIT          |
| Requests                 | Apache 2.0          |
| paho_mqtt          | EPL v2 / EDL v1          |
