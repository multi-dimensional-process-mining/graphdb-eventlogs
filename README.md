# Using Graph Databases for Event Data

## Description
This repository collects queries for modeling, importing, and analyzing event data as Knowledge Graphs using the Labeled Property Graph data model of graph databases. All scripts and queries are licensed under LGPL v3.0, see LICENSE. Copyright information is provided within each Project.

## Projects
The following projects are part of this repository


### csv_to_eventgraph_neo4j
Data model and generic query templates for translating and integrating a set of related CSV event logs into single event graph over multiple behavioral dimensions, stored as labeled property graph in [Neo4J](https://neo4j.com/). See [csv_to_eventgraph_neo4j/README.txt](a_scripts/README.txt)

Publications:
- Stefan Esser, Dirk Fahland: Multi-Dimensional Event Data in Graph Databases. [CoRR abs/2005.14552](https://arxiv.org/abs/2005.14552), [Journal on Data Semantics, DOI: 10.1007/s13740-021-00122-1](https://dx.doi.org/10.1007/s13740-021-00122-1) (2020)
- Esser, Stefan. (2020, February 19). A Schema Framework for Graph Event Data. Master thesis. Eindhoven University of Technology. https://doi.org/10.5281/zenodo.3820037


### exploration_bpic2017
Implementation of an explorative case study to model the BPI Challenge 2017 data sets using Neo4J. Publications:
- Capita Selecta report over "[Using Graph Data Structures for Event Logs](https://doi.org/10.5281/zenodo.3333831)" 
- Stefan Esser, Dirk Fahland: [Storing and Querying Multi-dimensional Process Event Logs Using Graph Databases](https://doi.org/10.1007/978-3-030-37453-2_51). Business Process Management Workshops 2019: 632-644
