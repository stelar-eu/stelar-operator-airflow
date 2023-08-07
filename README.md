# stelar-operator-airflow

## Overview

Stelar Operator is a necessary component to design and implement workflows inside the STELAR KLMS. This implementation refers to workflows that are designed with Airflow. The Operator is an extension of Airflow's BaseOperator and it basically communicates with all underlying components of the KLMS:
1. It communicates with the catalog to fetch the paths of datasets.
2. It communicates with a STELAR tool, which has been deployed as a service and receives / sends data by an API.
3. It publishes output data to the catalog.
4. It tracks parameters and metrics to the tracking server.

## Setup
To use the Stelar Operator, the user must install and setup all necessary components of STELAR KLMS. More information on that can be found [here](https://github.com/stelar-eu/klms-core-components-setup) and [here](https://github.com/stelar-eu/data-api).

## Examples

We have prepared a demo workflow where the user can see how to design workflows with Stelar Operator. In this workflow, there are two operators: the first one downloads daily articles from [GDELT](https://www.gdeltproject.org/) and filters them to include only datasets related to food incidents and the second one performs a deduplication using [TokenJoin](https://github.com/alexZeakis/TokenJoin)

### Setup
First, the user must deploy all existing tools. For this example we have two tools:
1. The downloader found in ``/tools/download_gdelt`` and can be initiated by ``python /tools/download_gdelt/main_service.py``. This service runs under port 9066. If needed, the user must change it by hand.
2. The deduplicator found in ``/tools/deduplicate_gdelt`` and can be initiated by ``python /tools/deduplicate_gdelt/main_service.py``. This service runs under port 9067. Again, if needed, the user must change it by hand.
3. Finally, the workflow is found under ``/examples/food_incidents_articles/dag.py``. The content of the ``/examples/`` must be placed under the directory which airflow expected to discover dags, by default called ``dags/``. Before that, the user must change all necessary information in the ``dag.py``, which are the ports that the tools can be found and the necessary information to communicate with the catalog and the data storage. These steps can be traced [here](https://github.com/stelar-eu/klms-core-components-setup).

### Results
If all work in perfection, the user will see completed dag_runs in the Airflow platform, published datasets in the catalog, published files in the data storage and tracked parameters and metrics in the tracking server.
