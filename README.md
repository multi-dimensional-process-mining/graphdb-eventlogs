# Using Graph Databases for Event Data

## Description

This repository collects queries for modeling, importing, and analyzing event data as Knowledge Graphs using the Labeled
Property Graph data model of graph databases. All scripts and queries are licensed under LGPL v3.0, see LICENSE.
Copyright information is provided within each Project.

## Requirements

Install neo4j-python-driver
pip install neo4j
OR
conda install -c conda-forge neo4j-python-driver

Install Neo4j from https://neo4j.com/download/

- Neo4j Desktop https://neo4j.com/download-center/#desktop (recommended), or
- Neo4j Community Server, https://neo4j.com/download-center/#community

## Get started

### Create a new graph database

- The scripts in this release assume password "1234".
- The scripts assume the server to be available at the default URL bolt://localhost:7687
- You can modify this also in the script.
- ensure to allocate enough memory to your database, advised: dbms.memory.heap.max_size=5G
- the script expects the Neo4j APOC library to be installed as a plugin, see https://neo4j.com/labs/apoc/

### Data set specific information
We provide data and scripts for

- BPIC14
- BPIC15
- BPIC16
- BPIC17
- BPIC19

For each of the datasets, we provide

- **data/.BPICXX/** - directory contains the original data in CSV format
  The datasets are available from:

            Esser, Stefan, & Fahland, Dirk. (2020). Event Data and Queries
            for Multi-Dimensional Event Data in the Neo4j Graph Database
            (Version 1.0) [Data set]. Zenodo. 
            http://doi.org/10.5281/zenodo.3865222
- **json_files/BPICXX.json** - json file that contains the semantic header for BPICXX
- **json_files/BPICXX_DS.json** - json file that contains a description for the different datasets for BPICXX (event
  tables etc)

- **a_scripts/file_preparation/bpicXX_prepare.py** - normalizes the original CSV data to an event table in CSV
  format required for the import and stores the output in the directory _ROOT/data/.BPICXX/prepared/_

### main script
There is one script that creates the Event/System knowledge graph: **a_scripts/main.py**

script to let Neo4J read the normalized event table of BPICXX from CSV files and executes several data modeling queries to construct
an event graph using 

- node types :Event, :Log, :Entity, :Class
- relationship types:
- :HAS (Log to Event, events recorded in a log),
- :CORR (Event to Entity, describing to which entities
an event is correlated),
- :DF (directly-follows of events: temporal ordering of
event nodes per corelated entity)
- :OBSERVED (Event to Event Class, which class of
events was observed when the event occurred,
e.g., which activity)
- :DF_C (aggregated directly-follows relation between
class nodes)
- :REL (Entity to Entity, which entities are structurally
related)

How to use
----------

For data import

1. start the Neo4j server
2. run bpicXX_prepare.py
3. set dataset_name to BPICXX in main.py and set use_sample to True/False
4. run main.py

## Projects

The following projects are part of this repository


### semantic header (json files)
First version for semantic header for system/event knowledge graphs: https://multiprocessmining.org/2022/10/26/data-storage-vs-data-semantics-for-object-centric-event-data/

### event knowledge graphs

Data model and generic query templates for translating and integrating a set of related CSV event logs into single event
graph over multiple behavioral dimensions, stored as labeled property graph in [Neo4J](https://neo4j.com/).
See [csv_to_eventgraph_neo4j/README.txt](a_scripts/README.txt)

Publications:

- Stefan Esser, Dirk Fahland: Multi-Dimensional Event Data in Graph
  Databases. [CoRR abs/2005.14552](https://arxiv.org/abs/2005.14552), [Journal on Data Semantics, DOI: 10.1007/s13740-021-00122-1](https://dx.doi.org/10.1007/s13740-021-00122-1) (
  2020)
- Esser, Stefan. (2020, February 19). A Schema Framework for Graph Event Data. Master thesis. Eindhoven University of
  Technology. https://doi.org/10.5281/zenodo.3820037

### exploration_bpic2017

Implementation of an explorative case study to model the BPI Challenge 2017 data sets using Neo4J. Publications:

- Capita Selecta report over "[Using Graph Data Structures for Event Logs](https://doi.org/10.5281/zenodo.3333831)"
- Stefan Esser, Dirk
  Fahland: [Storing and Querying Multi-dimensional Process Event Logs Using Graph Databases](https://doi.org/10.1007/978-3-030-37453-2_51).
  Business Process Management Workshops 2019: 632-644
