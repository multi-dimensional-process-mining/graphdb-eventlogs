# loan application

import pandas as pd
import time
import csv
from neo4j import GraphDatabase

# ## begin config
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
# Neo4j can import local files only from its own import directory,
# see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/
# Neo4j's default configuration enables import from local file directory
#    if it is not enabled, change Neo4j'c configuration file: dbms.security.allow_csv_import_from_file_urls=true
# Neo4j's default import directory is <NEO4J_HOME>/import, 
#    to use this script
#    - EITHER change the variable path_to_neo4j_import_directory to
#    <NEO4J_HOME>/import and move the input files to this directory
#    - OR set the import directory in Neo4j's configuration file: dbms.directories.import=
#    see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/#query-load-csv-introduction
# path_to_neo4j_import_directory = 'C:\\Temp\\Import\\'
path_to_neo4j_import_directory = 'C:\\Users\\avasw\\.Neo4jDesktop\\relate-data\\dbmss\\' \
                                 'dbms-a742a5ee-d1bb-45c5-9bb1-afa291d5c34b\\import\\'
# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised

# the script supports loading a small sample or the full log
step_Sample = True
if step_Sample:
    file_name = 'BPIC17sample.csv'
    perf_file_name = 'BPIC17samplePerformance.csv'
else:
    file_name = 'BPIC17full.csv'
    perf_file_name = 'BPIC17fullPerformance.csv'

# data model specific to BPIC17
data_set = 'BPIC17'

include_entities = ['Application', 'Workflow', 'Offer', 'Case_R', 'Case_AO', 'Case_AW', 'Case_WO']
# include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO','Case_AWO']


model_entities = [['Application', 'case', 'WHERE e.EventOrigin = "Application"'],  # individual entities
                  ['Workflow', 'case', 'WHERE e.EventOrigin = "Workflow"'],
                  ['Offer', 'OfferID', 'WHERE e.EventOrigin = "Offer"'],
                  ['Case_R', 'resource', 'WHERE EXISTS(e.resource)'],  # resource as entity
                  ['Case_AWO', 'case', 'WHERE EXISTS(e.case)']]  # original case notion

# specification of relations between entities
#    1 name of the relation
#    2 name of first entity, 
#    3 name of second entity where events have a property referring to the first entity, i.e., a foreign key
#    4 name of the foreign key property by which events of the second entity refer to the first entity
model_relations = [['Case_AO', 'Application', 'Offer', 'case'],
                   ['Case_AW', 'Application', 'Workflow', 'case'],
                   ['Case_WO', 'Workflow', 'Offer', 'case']]

# specification of entities to derive by reifying relations: 
#    1 name of the relation in 'model_relations' that shall be reified
model_entities_derived = ['Case_AO',
                          'Case_AW',
                          'Case_WO']

# several steps of import, each can be switch on/off
step_ClearDB = True  # entire graph shall be cleared before starting a new import
step_LoadEventsFromCSV = True  # import all (new) events from CSV file
step_FilterEvents = False  # filter events prior to graph construction
step_createLog = True  # create log nodes and relate events to log node
step_createEntities = True  # create entities from identifiers in the data as specified in this script
step_createEntityRelations = True  # create foreign-key relations between entities
step_reifyRelations = True  # reify relations into derived entities
step_createDF = True  # compute directly-follows relation for all entities in the data
step_deleteParallelDF = True  # remove directly-follows relations for derived entities that run
# in parallel with DF-relations for base entities
step_createEventClasses = True  # aggregate events to event classes from data
step_createDFC = False  # aggregate directly-follows relation to event classes
step_createHOWnetwork = False  # create resource activity classifier and HOW network

option_filter_removeEventsWhere = 'WHERE e.lifecycle in ["SUSPEND","RESUME"]'

option_DF_entity_type_in_label = False  # set to False when step_createDFC is enabled

option_Contains_Lifecycle_Information = True  # whether events hold attribute "Lifecycle" to be used in event classifiers


# ## end config

######################################################
# ############ DEFAULT METHODS AND QUERIES ############
######################################################

# load data from CSV and import into graph
def load_log(local_file):
    dataset_list = []
    header_csv = []
    i = 0
    with open(local_file) as f:
        reader = csv.reader(f)
        for row in reader:
            if i == 0:
                header_csv = list(row)
                i += 1
            else:
                dataset_list.append(row)

    log = pd.DataFrame(dataset_list, columns=header_csv)

    return header_csv, log


# create events from CSV table: one event node per row, one property per column
def create_event_query(log_header, file_name, log_id=""):


    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///{file_name}\" as line'
    for col in log_header:
        if col == 'idx':
            column = f'toInt(line.{col})'
        elif col in ['timestamp', 'start', 'end']:
            column = f'datetime(line.{col})'
        else:
            column = 'line.' + col

        if log_header.index(col) == 0 and log_id != "":
            new_line = f' CREATE (e:Event {{Log: "{log_id}",{col}: {column},'
        elif log_header.index(col) == 0:
            new_line = f' CREATE (e:Event {{ {col}: {column},'
        else:
            new_line = f' {col}: {column},'
        if log_header.index(col) == len(log_header) - 1:
            new_line = f' {col}: {column} }})'

        query = query + new_line
    return query


# run query for Neo4J database
def run_query(query):
    with driver.session() as session:
        result = session.run(query).single()
        if result is not None:
            return result.value()
        else:
            return None


def filter_events(tx, condition):
    q_filter_events = f'MATCH (e:Event) {condition} DELETE e'
    print(q_filter_events)
    tx.run(q_filter_events)


def add_log(tx, log_id):
    q_create_log = f'CREATE (:Log {{ID: "{log_id}" }})'
    print(q_create_log)
    tx.run(q_create_log)

    q_link_events_to_log = f'''
            MATCH (e:Event {{Log: "{log_id}" }}) 
            MATCH (l:Log {{ID: "{log_id}" }}) 
            CREATE (l)-[:HAS]->(e)'''
    print(q_link_events_to_log)
    tx.run(q_link_events_to_log)


def create_entity(tx, entity_type, entity_id, where_event_property):
    q_create_entity = f'''
            MATCH (e:Event) {where_event_property}
            WITH e.{entity_id} AS id
            MERGE (en:Entity {{ID:id, uID:("{entity_type}"+toString(id)), EntityType:"{entity_type}" }})'''
    print(q_create_entity)
    tx.run(q_create_entity)


def create_entity_relationships(tx, relation_type, entity_type1, entity_type2, reference_from1to2):
    q_create_relation = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:Entity ) WHERE n1.EntityType="{entity_type1}"
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:Entity ) WHERE n2.EntityType="{entity_type2}"
                AND n1 <> n2 AND e2.{reference_from1to2} = n1.ID
            WITH DISTINCT n1,n2
            CREATE ( n1 ) <-[:REL {{Type:"{relation_type}"}} ]- ( n2 )'''
    print(q_create_relation)
    tx.run(q_create_relation)


def reify_entity_relations(tx, relation_type):
    q_reify_relation = f'''
            MATCH ( n1 : Entity ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:Entity )
            CREATE (n1) <-[:REL {{Type:"Reified"}}]- (new : Entity {{ 
                ID:toString(n1.ID)+"_"+toString(n2.ID),
                EntityType: "{relation_type}",
                uID:"{relation_type}"+toString(n1.ID)+"_"+toString(n2.ID) }} )
                -[:REL {{Type:"Reified"}}]-> (n2)'''
    print(q_reify_relation)
    tx.run(q_reify_relation)


def correlate_events_to_entity(tx, entity_type, entity_id, where_event_property):
    q_correlate = f'''
            MATCH (e:Event) {where_event_property}
            MATCH (n:Entity {{EntityType: "{entity_type}" }}) WHERE e.{entity_id} = n.ID
            CREATE (e)-[:CORR]->(n)'''
    print(q_correlate)
    tx.run(q_correlate)


def correlate_events_to_derived_entity(tx, derived_entity_type):
    q_correlate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REL {{Type:"Reified"}}]- 
                (r:Entity {{EntityType:"{derived_entity_type}"}} )
            CREATE (e)-[:CORR]->(r)'''
    print(q_correlate)
    tx.run(q_correlate)


def create_directly_follows(tx, entity_type, option_df_entity_type_in_label):
    q_create_df = f'''
        MATCH ( n : Entity ) WHERE n.EntityType="{entity_type}"
        MATCH ( n ) <-[:CORR]- ( e )
        
        WITH n , e as nodes ORDER BY e.timestamp,ID(e)
        WITH n , collect ( nodes ) as nodeList
        UNWIND range(0,size(nodeList)-2) AS i
        WITH n , nodeList[i] as first, nodeList[i+1] as second'''
    q_create_df = q_create_df + '\n'

    if option_df_entity_type_in_label == True:
        q_create_df = q_create_df + f'MERGE ( first ) -[df:DF_{entity_type}]->( second )'
    else:
        q_create_df = q_create_df + f'MERGE ( first ) -[df:DF {{EntityType:n.EntityType}} ]->( second )'

    print(q_create_df)
    tx.run(q_create_df)


def delete_parallel_directly_follows_derived(tx, derived_entity_type, original_entity_type):
    if option_DF_entity_type_in_label:
        q_delete_df = f'''
            MATCH (e1:Event) -[df:DF_{derived_entity_type}]-> (e2:Event)
            WHERE (e1:Event) -[:DF_{original_entity_type}]-> (e2:Event)
            DELETE df'''
    else:
        q_delete_df = f'''
            MATCH (e1:Event) -[df:DF {{EntityType: "{derived_entity_type}" }}]-> (e2:Event)
            WHERE (e1:Event) -[:DF {{EntityType: "{derived_entity_type}" }}]-> (e2:Event)
            DELETE df'''

    print(q_delete_df)
    tx.run(q_delete_df)


def create_event_class_activity(tx):
    q_create_ec = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName
        MERGE ( c : Class {{ Name:actName, Type:"Activity", ID: actName}})'''
    print(q_create_ec)
    tx.run(q_create_ec)

    q_link_event_to_class = f'''
        MATCH ( c : Class ) WHERE c.Type = "Activity"
        MATCH ( e : Event ) WHERE c.Name = e.Activity
        CREATE ( e ) -[:OBSERVED]-> ( c )'''
    print(q_link_event_to_class)
    tx.run(q_link_event_to_class)


def create_event_class_activity_and_lifecycle(tx):
    q_create_ec = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName,e.lifecycle AS lifecycle
        MERGE ( c : Class {{ Name:actName, Lifecycle:lifecycle, Type:"Activity+Lifecycle", ID: actName+"+"+lifecycle}})'''
    print(q_create_ec)
    tx.run(q_create_ec)

    q_link_event_to_class = f'''
        MATCH ( c : Class ) WHERE c.Type = "Activity+Lifecycle"    
        MATCH ( e : Event ) where e.Activity = c.Name AND e.lifecycle = c.Lifecycle
        CREATE ( e ) -[:OBSERVED]-> ( c )'''
    print(q_link_event_to_class)
    tx.run(q_link_event_to_class)


def create_event_class_resource(tx):
    q_create_ec = f'''
        MATCH ( e : Event ) WITH distinct e.resource AS name
        MERGE ( c : Class {{ Name:name, Type:"Resource", ID: name}})'''
    print(q_create_ec)
    tx.run(q_create_ec)

    q_link_event_to_class = f'''
        MATCH ( e : Event )
        MATCH ( c : Class ) WHERE c.Type = "Resource" AND c.ID = e.resource
        CREATE ( e ) -[:OBSERVED]-> ( c )'''
    print(q_link_event_to_class)
    tx.run(q_link_event_to_class)


def aggregate_all_df_relations(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifier between the same entity
    q_create_dfc = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(q_create_dfc)
    tx.run(q_create_dfc)


def aggregate_df_relations(tx, entity_type, event_cl):
    # aggregate only for a specific entity type and event classifier
    q_create_dfc = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(q_create_dfc)
    tx.run(q_create_dfc)


def aggregate_df_relations_for_entities(tx, entity_types, event_cl):
    # aggregate only for a specific entity type and event classifier
    q_create_dfc = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = df.EntityType AND df.EntityType IN {entity_types} AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(q_create_dfc)
    tx.run(q_create_dfc)


def aggregate_df_relations_filtering(tx, entity_type, event_cl, df_threshold, relative_df_threshold):
    # aggregate only for a specific entity type and event classifier
    # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
    q_create_dfc = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
        WHERE df_freq > {df_threshold}
        OPTIONAL MATCH ( c2 : Class ) <-[:OBSERVED]- ( e2b : Event ) -[df2:DF]-> ( e1b : Event ) -[:OBSERVED]-> ( c1 : Class )
        WITH EntityType as EType,c1,df_freq,count(df2) AS df_freq2,c2
        WHERE (df_freq*{relative_df_threshold} > df_freq2)
        MERGE ( c1 ) -[rel2:DF_C  {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    print(q_create_dfc)
    tx.run(q_create_dfc)


###############################################################################################
####################### Standard Script for Loading CSV Files into Neo4j ######################
####################### based on configuration at the top of this file   ######################
###############################################################################################

if step_ClearDB:  ### delete all nodes and relations in the graph to start fresh
    print('Clearing DB...')

    # run one delete transaction per relationship type: smaller transactions require less memory and execute faster
    relationTypes = [":DF", ":CORR", ":OBSERVED", ":HAS", ":DF_C", ":REL"]
    for relType in relationTypes:
        qDeleteRelation = f'''MATCH () -[r{relType}]- () DELETE r'''
        print(qDeleteRelation)
        run_query(qDeleteRelation)
    # delete all remaining relationships
    qDeleteAllRelations = "MATCH () -[r]- () DELETE r"
    run_query(qDeleteAllRelations)

    # run one delete transaction per node type type: smaller transactions require less memory and execute faster
    nodeTypes = [":Event", ":Entity", ":Log", ":Class"]
    for nodeType in nodeTypes:
        qDeleteNodes = f'''MATCH (n{nodeType}) DELETE n'''
        print(qDeleteNodes)
        run_query(qDeleteNodes)
    # delete all remaining relationships
    qDeleteAllNodes = "MATCH (n) DELETE n"
    run_query(qDeleteAllNodes)

# table to measure performance
perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
start = time.time()
last = start

if step_LoadEventsFromCSV:
    print('Import events from CSV')
    # load CSV tables
    header, csvLog = load_log(path_to_neo4j_import_directory + file_name)
    # convert each record in the CSV table into an Event node
    qCreateEvents = create_event_query(header, file_name,
                                     'BPIC17')  # generate query to create all events with all log columns as properties
    run_query(qCreateEvents)

    # create unique constraints
    run_query('CREATE CONSTRAINT ON (e:Event) ASSERT e.ID IS UNIQUE;')  # for implementation only (not required by schema or patterns)
    run_query('CREATE CONSTRAINT ON (en:Entity) ASSERT en.uID IS UNIQUE;')  # required by core pattern
    run_query('CREATE CONSTRAINT ON (l:Log) ASSERT l.ID IS UNIQUE;')  # required by core pattern

    end = time.time()
    perf = perf.append({'name': data_set + '_event_import', 'start': last, 'end': end, 'duration': (end - last)},
                       ignore_index=True)
    print('Event nodes done: took ' + str(end - last) + ' seconds')
    last = end

if step_FilterEvents:
    print('Filtering events')
    with driver.session() as session:
        session.write_transaction(filter_events, option_filter_removeEventsWhere)

    end = time.time()
    perf = perf.append({'name': data_set + '_filter_events', 'start': last, 'end': end, 'duration': (end - last)},
                       ignore_index=True)
    print('Filter event nodes done: took ' + str(end - last) + ' seconds')
    last = end

##create log node and :HAS relationships
if step_createLog:
    with driver.session() as session:
        session.write_transaction(add_log, data_set)

    end = time.time()
    perf = perf.append({'name': data_set + '_create_log', 'start': last, 'end': end, 'duration': (end - last)},
                       ignore_index=True)
    print('Log and :HAS relationships done: took ' + str(end - last) + ' seconds')
    last = end

##create entities
if step_createEntities:
    for entity in model_entities:  # per entity
        if entity[0] in include_entities:
            with driver.session() as session:
                session.write_transaction(create_entity, entity[0], entity[1], entity[2])
                print(f'{entity[0]} entity nodes done')
                session.write_transaction(correlate_events_to_entity, entity[0], entity[1], entity[2])
                print(f'{entity[0]} E_EN relationships done')

            end = time.time()
            perf = perf.append(
                {'name': data_set + '_create_entity ' + entity[0], 'start': last, 'end': end, 'duration': (end - last)},
                ignore_index=True)
            print('Entity ' + entity[0] + ' done: took ' + str(end - last) + ' seconds')
            last = end

## create relationships between entities
if step_createEntityRelations:
    for relation in model_relations:  # per relation
        with driver.session() as session:
            session.write_transaction(create_entity_relationships, relation[0], relation[1], relation[2], relation[3])
            print(f'{relation[0]} relationships created')

        end = time.time()
        perf = perf.append({'name': data_set + '_create_entity_relationships ' + relation[0], 'start': last, 'end': end,
                            'duration': (end - last)}, ignore_index=True)
        print('Entity ' + relation[0] + ' done: took ' + str(end - last) + ' seconds')
        last = end

if step_reifyRelations:
    for relation in model_relations:  # per relation
        derived_entity = relation[0]
        if derived_entity in model_entities_derived and derived_entity in include_entities:
            with driver.session() as session:
                session.write_transaction(reify_entity_relations, derived_entity)
                print(f'{derived_entity} relationships reified')
                session.write_transaction(correlate_events_to_derived_entity, derived_entity)
                print(f'{derived_entity} E_EN relationships created')

            end = time.time()
            perf = perf.append({'name': data_set + '_reify_relationships ' + derived_entity, 'start': last, 'end': end,
                                'duration': (end - last)}, ignore_index=True)
            print('Entity ' + derived_entity + ' done: took ' + str(end - last) + ' seconds')
            last = end

if step_createDF:
    for entity in include_entities:  # per entity
        with driver.session() as session:
            session.write_transaction(create_directly_follows, entity, option_DF_entity_type_in_label)

        end = time.time()
        perf = perf.append(
            {'name': data_set + '_create_df ' + entity, 'start': last, 'end': end, 'duration': (end - last)},
            ignore_index=True)
        print('DF for Entity ' + entity + ' done: took ' + str(end - last) + ' seconds')
        last = end

if step_deleteParallelDF:
    for relation in model_relations:  # per relation
        derived_entity = relation[0]
        if derived_entity not in include_entities or derived_entity not in model_entities_derived:
            continue

        parent_entity = relation[1]
        child_entity = relation[2]

        with driver.session() as session:
            # entities are derived from 2 other entities, delete parallel relations wrt. to those
            session.write_transaction(delete_parallel_directly_follows_derived, derived_entity, parent_entity)
            session.write_transaction(delete_parallel_directly_follows_derived, derived_entity, child_entity)

        end = time.time()
        perf = perf.append({'name': data_set + '_delete_parallel_df ' + derived_entity, 'start': last, 'end': end,
                            'duration': (end - last)}, ignore_index=True)
        print('Remove parallel DF for Entity ' + derived_entity + ' done: took ' + str(end - last) + ' seconds')
        last = end

if step_createEventClasses:
    with driver.session() as session:
        session.write_transaction(create_event_class_activity)
        if option_Contains_Lifecycle_Information:
            session.write_transaction(create_event_class_activity_and_lifecycle)

    end = time.time()
    perf = perf.append({'name': data_set + '_create_classes', 'start': last, 'end': end, 'duration': (end - last)},
                       ignore_index=True)
    print('Event classes done: took ' + str(end - last) + ' seconds')
    last = end

if step_createDFC:
    for entity in include_entities:
        with driver.session() as session:
            if option_Contains_Lifecycle_Information:
                classifier = "Activity+Lifecycle"
            else:
                classifier = "Activity"
            # session.write_transaction(aggregateDFrelationsFiltering,entity,classifier,5000,3)
            # session.write_transaction(aggregateDFrelationsFiltering,entity,classifier,1,3)
            session.write_transaction(aggregate_df_relations, entity, classifier)

        end = time.time()
        perf = perf.append(
            {'name': data_set + '_aggregate_df_' + entity, 'start': last, 'end': end, 'duration': (end - last)},
            ignore_index=True)
        print('Aggregating DF for ' + entity + ' done: took ' + str(end - last) + ' seconds')
        last = end

if step_createHOWnetwork:
    with driver.session() as session:
        session.write_transaction(create_event_class_resource)
        # create HOW relations along all process entities, except Case_R
        how_entities = include_entities
        how_entities.remove("Case_R")
        session.write_transaction(aggregate_df_relations_for_entities, how_entities, "Resource")

    end = time.time()
    perf = perf.append({'name': data_set + '_create_how', 'start': start, 'end': end, 'duration': (end - last)},
                       ignore_index=True)
    print('Creating HOW network done: took ' + str(end - last) + ' seconds')
    last = end

end = time.time()
perf = perf.append({'name': data_set + '_total', 'start': start, 'end': end, 'duration': (end - start)},
                   ignore_index=True)

perf.to_csv(perf_file_name)
driver.close()
