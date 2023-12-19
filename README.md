# Data Enrichment Toolchain by Universidad de Cantabria

## Introduction

#### 📝 Description
This repository contains the DET (Data Enrichment Toolchain) components developed by Universidad de Cantabria (UC) within the [SALTED project](https://salted-project.eu/). This includes collectors, a mapper, a curator, and several linkers and enrichers.

#### :arrow_forward: Workflow
The DET starts by collecting data from heterogeneous sources, and goes all the way to publishing the harmonised enriched data in an NGSI-LD Context Broker. Below is a workflow briefly explaining what each component does:
- The Data Collectors (*collector*, *sms* and *kcity* folders) harvest data from heterogeneous sources by way of several protocols, such as HTTP or MQTT.
- The Data Mapper (*mapper* folder) transforms heterogeneous formats into NGSI-LD, and more specifically Smart Data Models.
- The Data Curator (*curator* folder) assesses the quality of the data and tags the NGSI-LD entities accordingly.
- The Data Linkers and Enrichers (*enricher* folder) add value to existing data by adding new entities, properties or relationships.

#### :sparkles: Other components
The Data Curator is based on the [Data Quality Assessment](https://github.com/lauramartingonzalezzz/DQAssessment) module developed by UC.

#### 📧 Contact
All code located in this repository has been developed by [Universidad de Cantabria](https://web.unican.es/) ([contact](https://salted-project.eu/contact/)).

## Installation
The most simple and effective way of installing the UC DET is by using the provided *docker-compose.yml* file. Before installing it, however, you may want to adapt the configuration to your particular needs. You can do that by modifying the *config.conf* file. Next you can run the command:
```bash
docker compose up -d --build
```
All components will be up and running. You can interact with them, check the docker logs, or set up an NGSI-LD Context Broker to use their full potential.

## Acknowledgement
This work was supported by the European Commission CEF Programme by means of the project SALTED ‘‘Situation-Aware Linked heTerogeneous Enriched Data’’ under the Action Number 2020-EU-IA-0274.

## License
This material is licensed under the GNU Lesser General Public License v3.0 whose full text may be found at the *LICENSE* file.