Data model and generic query templates for translating and integrating a set
of related CSV event logs into a single event knowledge (EKG), stored as 
labeled property graph in KuzuDB.

Copyright (C) 2019-2025
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
Install kuzudb (in-memory graph database as a Python package)
	pip install kuzudb

Database creation and launching is fully managed by the Python scripts, no other setup or configuration is required.

Data and scripts provided
-------------------------

We provide data and script for 
- BPIC17

For each of the datasets, we provide

/.BPICXX/ - directory contains the original data in CSV format
            The datasets are available from:
            
            Esser, Stefan, & Fahland, Dirk. (2020). Event Data and Queries
            for Multi-Dimensional Event Data in the Neo4j Graph Database
            (Version 1.0) [Data set]. Zenodo. 
            http://doi.org/10.5281/zenodo.3865222

bpicXX_prepare.py - normalizes the original CSV data to an event table in CSV
                    format required for the import and stores the output in
                    the directory ./prepared/

bpicXX_import_csv_to_kuzu_db.py - 
                  script to let KuzuDB read the normalized event table of
                  BPICXX from CSV files in './prepared/' 
                  and executes several data modeling queries to construct
                  an event knowledge graph using 
                   - node types :Event, :Log, :Entity
                   - relationship types:
                     - :HAS (Log to Event, events recorded in a log),
                     - :CORR (Event to Entity, describing to which entities
                       an event is correlated), 
                     - :REL (Entity to Entity, which entities are structurally
                       related)

                  - invokes infer_df_edges.py to infer :DF relations between
                    :Event nodes (see below)

                  - invokes queries_build_dfg.py to aggregate EKG into a
                    multi-entity directly-follows graph by adding :Class
                    nodes and :DF_C edges (see below)

infer_df_edges.py - generic inference of directly-follows relationships between
                    all :Event nodes related (:CORR) to the same :Entity node
                     - :DF (directly-follows of events: temporal ordering of
                       event nodes per corelated entity)

queries_build_dfg.py - 
                  generic inference of multi-entity directly-follows graph
                  for existing EKG by adding
                   - node type :Class
                   - relationship types
                     - :OBSERVED (Event to Event Class, which class of
                       events was observed when the event occurred, 
                       e.g., which activity)
                     - :DF_C (aggregated directly-follows relation between
                       class nodes)

..._typed.py - variants of scripts for importing CSV as EKG in KuzuDB as
               strictly types nodes and relations, e.g., using node labele
               :Application instead of :Entity node with property
               EntityType="Application"

How to use
----------

For data import

2. run bpicXX_prepare.py
3. run bpicXX_import.py


Examples of data querying
-------------------------

bpic17_queries_....py

Python scripts that query the graph of the BPIC17 dataset for properties and return GraphViz Dot visualizations of the quried graph.

- to be added