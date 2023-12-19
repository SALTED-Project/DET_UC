# Data Curator by Universidad de Cantabria
This repository contains the source code of the Data Curator developed by Universidad de Cantabria (UC) within the framework of the [SALTED project](https://salted-project.eu/). This component assesses several data quality dimensions of the data, such as accuracy, precision, timeliness or completeness, and generates Data Quality Assessment NGSI-LD entities. Moreover, it performs outlier detection and imputation of missing values in temperature data-streams and tags the output entities accordingly. The output is injected into an NGSI-LD Context Broker.

Note: The *src_quality_assessment/files/training_data.csv* file needs to be filled with no more than 100 rows of training data. Each row should be a temperature data point collected every 5 minutes.

## Acknowledgement
This work was supported by the European Commission CEF Programme by means of the project SALTED ‘‘Situation-Aware Linked heTerogeneous Enriched Data’’ under the Action Number 2020-EU-IA-0274.

## License
This material is licensed under the GNU Lesser General Public License v3.0 whose full text may be found at the *LICENSE* file located in the root of the repository.

It mainly makes use of the following libraries and frameworks (dependencies of dependencies have been omitted):

| Library / Framework |   Licence    |
|---------------------|--------------|
| geopy             | MIT          |
| numpy                 | BSD-3-Clause           |
| paho_mqtt          | EPL v2 / EDL v1          |
| pandas          | BSD-3-Clause          |
| python_dateutil          | Apache 2.0 and BSD-3-Clause          |
| pytz             | MIT          |
| Requests                 | Apache 2.0          |
| Seasonal Anomaly Detection - Streaming Data | Apache 2.0  |
