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

We have prepared some workflows where the user can see how to design workflows with Stelar Operator. In the [demos section](https://github.com/stelar-eu/stelar-operator-airflow/tree/main/workflows/demos/), the user can find demo workflows on how to use one of the KLMS tools individually and in the [use-case section](https://github.com/stelar-eu/stelar-operator-airflow/tree/main/workflows/use_cases/) the user can find implementations of the existing use-cases, where the KLMS tools are combined together.

More information about the KMLS tools can be found [here](https://github.com/stelar-eu/stelar-operator-airflow/tree/main/tools/).
