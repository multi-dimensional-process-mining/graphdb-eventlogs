Data model and generic query templates for translating and integrating a set
of related CSV event logs into a single event graph over multiple behavioral
dimensions, stored as labeled property graph in Neo4J.

Copyright (C) 2019-2021
Stefan Esser, INFORM GmbH, Aachen, Germany and Eindhoven University of Technology, Eindhoven, the Netherlands
Dirk Fahland, Eindhoven University of Technology, Eindhoven, the Netherlands

The work is licensed under the LGPL v3.0, see file LICENSE

Supporting Publications
-----------------------

Esser, Stefan and Dirk Fahland. "Multi-Dimensional Event Data in Graph Databases." (2021)
Journal on Data Semantics. DOI: 10.1007/s13740-021-00122-1
arXiv pre-print: https://arxiv.org/pdf/2005.14552.pdf


Requirements
------------

Install Python/Anaconda
Install neo4j-python-driver
	pip install neo4j
		OR
	conda install -c conda-forge neo4j-python-driver

Install Neo4j from https://neo4j.com/download/
- Neo4j Desktop https://neo4j.com/download-center/#desktop (recommended), or
- Neo4j Community Server, https://neo4j.com/download-center/#community

Create a new graph database
- The scripts in this release assume password "1234".
- The scripts assume the server to be available 
  at the default URL bolt://localhost:7687
- You can modify this also in the script.
- ensure to allocate enough memory to your database,
  advised: dbms.memory.heap.max_size=20G

Data and scripts provided
-------------------------

We provide data and script for 
- BPIC14
- BPIC15
- BPIC16
- BPIC17
- BPIC19

For each of the datasets, we provide

/.BPICXX/ - directory contains the original data in CSV format
            The datasets are available from:
            
            Esser, Stefan, & Fahland, Dirk. (2020). Event Data and Queries
            for Multi-Dimensional Event Data in the Neo4j Graph Database
            (Version 1.0) [Data set]. Zenodo. 
            http://doi.org/10.5281/zenodo.3865222

bpicXX_prepare.py - normalizes the original CSV data to an event table in CSV
                    format required for the import and stores the output in
                    the directory specified
                    in variable 'path_to_neo4j_import_directory'
                    (see "Configuration" below)

bpicXX_import.py - script to let Neo4J read the normalized event table of
                   BPICXX from CSV files in 'path_to_neo4j_import_directory' 
                   and executes several data modeling queries to construct
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
3. run bpicXX_import.py


Examples of data querying
-------------------------

bpic17_queries_....py

Python scripts that query the graph of the BPIC17 dataset for properties and return GraphViz Dot visualizations of the quried graph.

- bpic17_queries_show_cases... assumes bpic17_import.py was run with
  - include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO']
  - option_DF_entity_type_in_label = True

- bpic17_queries_show_aggregated-df... assumes bpic17_import.py was run with
  - option_DF_entity_type_in_label = False
  - include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO','Case_AWO']
  - step_createDFC = True

bpic17_queries_...cypher.txt

Plain Cypher queries to query the BPIC17 graph for properties of the DF relationships.

        
Configuration
-------------

All configuration variables are set at the start of each script.                   

# Neo4j can import local files only from its own import directory, see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/
# Neo4j's default configuration enables import from local file directory
#    if it is not enabled, change Neo4j'c configuration file: dbms.security.allow_csv_import_from_file_urls=true
# Neo4j's default import directory is <NEO4J_HOME>/import, 
#    to use this script
#    - EITHER change the variable path_to_neo4j_import_directory to <NEO4J_HOME>/import and move the input files to this directory
#    - OR set the import directory in Neo4j's configuration file: dbms.directories.import=
#    see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/#query-load-csv-introduction

ensure to allocate enough memory to your database, advised: dbms.memory.heap.max_size=20G
