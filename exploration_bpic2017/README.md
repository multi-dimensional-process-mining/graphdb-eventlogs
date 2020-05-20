# Using Graph Data Structures for Event Logs
## Description
This repository is a supplement to the Capita Selecta report over "[Using Graph Data Structures for Event Logs](https://doi.org/10.5281/zenodo.3333831)" to enable easy replication of its experiments.

## Software
The following software is recommended to be used for experiment replication:
- Python 3.6.8 (see [requirements.txt](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs/blob/master/requirements.txt) for libraries)
- Neo4j Desktop with Neo4j Database version 3.5.4 or higher (get it [here](https://neo4j.com/download/?ref=product))
- ProM Lite 1.2 (get it [here](http://promtools.org/doku.php?id=promlite12))

## Step-by-step
1. Get the BPI Challenge 2017 dataset [here](https://data.4tu.nl/repository/uuid:5f3067df-f10b-45da-b98b-86ae4c7a310b) and unzip it.
2. Use Prom Lite 1.2 to load the "BPI Challenge 2017.xes" and convert to CSV format with filename "bpiChallenge17.csv".
3. For the full dataset, copy the Python script "[dataPrepBPIC17full.py](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs/blob/master/dataPrepBPIC17full.py)" to the folder where the "bpiChallenge17.csv" file is located. For the sample set, use "[dataPrepBPIC17sample.py](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs/blob/master/dataPrepBPIC17sample.py)" respectively.
4. Execute the Python script, the output should be a CSV file named "loan_full.csv" (or "loan_sample.csv").
5. In Neo4j Desktop, create a new database 3.5.4 or higher and start it up.
6. Locate the import folder of the newly created database. On Windows default installation this might look similar to: "*C:\Users\username\.Neo4jDesktop\neo4jDatabases\database-9d1700fa-03f9-48e7-b8ad-6b7c25890ee2\installation-3.5.4\import*"  
7. Copy the "loan_full.csv" or "loan_sample.csv" to the import folder.
8. Open the Neo4j Browser to connect to the newly created Neo4j database and execute the Cypher commands in [CypherCreateGraph.txt](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs/blob/master/CypherCreateGraph.txt) or [CypherCreateGraphSample.txt](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs/blob/master/CypherCreateGraphSample.txt) in the indicated order to create the graph. **Note that the queries build up on each other. It is necessary to execute them one by one!** 
9. Use the Cypher queries in "[CypherAnalysis.txt](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs/blob/master/CypherAnalysis.txt)" and the "[EvaluationQuestion12.py](https://github.com/multi-dimensional-process-mining/graphdb-eventlogs/blob/master/EvaluationQuestion12.py)" script to analyze the graph. "EvaluationQuestion12.py" must be located in the same folder as "bpiChallenge17.csv".

## Final Remarks
It may occur, especially when working with the full dataset, that Neo4j runs into memory problems when executing certain queries. In this case try to tweak the "dbms.memory.heap.max_size" parameter in the database configuration file.
