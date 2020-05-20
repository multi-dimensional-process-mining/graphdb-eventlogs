#loan application

import pandas as pd
import time, csv
from neo4j import GraphDatabase

### begin config
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
# Neo4j can import local files only from its own import directory, see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/
path_to_neo4j_import_directory = 'D:\\data\\neo4j\\neo4jDatabases\\database-18dae032-83d2-4404-b74a-ba982dace54a\\installation-3.5.14\\import\\'
# ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised

# the script supports loading a small sample or the full log
step_Sample = False
if(step_Sample):
    fileName = 'BPIC17sample.csv' 
    perfFileName = 'BPIC17samplePerformance.csv'
else:
    fileName = 'BPIC17full.csv'
    perfFileName = 'BPIC17fullPerformance.csv'
    
# data model specific to BPIC17
dataSet = 'BPIC17'

model_entities = [['Application','case', 'WHERE e.EventOrigin = "Application"'], # individual entities
                  ['Workflow', 'case', 'WHERE e.EventOrigin = "Workflow"'],
                  ['Offer', 'OfferID', 'WHERE e.EventOrigin = "Offer"'],
                  ['Case_R', 'resource', 'WHERE EXISTS(e.resource)'], # resource as entity
                  ['Case_AWO','case', 'WHERE EXISTS(e.case)']] # original case notion

# specification of derived entities: 
#    1 name of derived entity, 
#    2 name of first entity, 
#    3 name of second entity where events have an property referring to the first entity, i.e., a foreign key
#    4 name of the foreign key property by which events of the second entity refer to the first entity
model_entities_derived = [['Case_AO','Application','Offer','case'],
                          ['Case_AW','Application','Workflow','case'],
                          ['Case_WO','Workflow','Offer','case']]
    
# several steps of import, each can be switch on/off
step_ClearDB = True           # entire graph shall be cleared before starting a new import
step_LoadEventsFromCSV = True # import all (new) events from CSV file
step_createLog = True         # create log nodes and relate events to log node
step_createEntities = True          # create entities from identifiers in the data as specified in this script
step_createEntitiesDerived = True   # create derived entities as specified in the script
step_createDF = True           # compute directly-follows relation for all entities in the data
step_createEventClasses = True # aggregate events to event classes from data
step_createDFC = True          # aggregate directly-follows relation to event classes
step_createHOWnetwork = True   # create resource activitiy classifier and HOW network

option_DF_entity_type_in_label = False
### end config

######################################################
############# DEFAULT METHODS AND QUERIES ############
######################################################

# load data from CSV and import into graph
def LoadLog(localFile):
    datasetList = []
    headerCSV = []
    i = 0
    with open(localFile) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i==0):
                headerCSV = list(row)
                i +=1
            else:
               datasetList.append(row)
        
    log = pd.DataFrame(datasetList,columns=headerCSV)
    
    return headerCSV, log

# create events from CSV table: one event node per row, one property per column
def CreateEventQuery(logHeader, fileName, LogID = ""):
    query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///{fileName}\" as line'
    for col in logHeader:
        if col == 'idx':
            column = f'toInt(line.{col})'
        elif col in ['timestamp','start','end']:
            column = f'datetime(line.{col})'
        else:
            column = 'line.'+col
        newLine = ''
        if (logHeader.index(col) == 0 and LogID != ""):
            newLine = f' CREATE (e:Event {{Log: "{LogID}",{col}: {column},'
        elif (logHeader.index(col) == 0):
            newLine = f' CREATE (e:Event {{ {col}: {column},'
        else:
            newLine = f' {col}: {column},'
        if (logHeader.index(col) == len(logHeader)-1):
            newLine = f' {col}: {column} }})'
            
        query = query + newLine
    return query;

# run query for Neo4J database
def runQuery(driver, query):
    with driver.session() as session:
        result = session.run(query).single()
        if result != None: 
            return result.value()
        else:
            return None
        
def add_log(tx, log_id):
    qCreateLog = f'CREATE (:Log {{ID: "{log_id}" }})'
    print(qCreateLog)
    tx.run(qCreateLog)

    qLinkEventsToLog = f'''
            MATCH (e:Event {{Log: "{log_id}" }}) 
            MATCH (l:Log {{ID: "{log_id}" }}) 
            CREATE (l)-[:L_E]->(e)'''
    print(qLinkEventsToLog)
    tx.run(qLinkEventsToLog)

def create_entity(tx, entity_type, entity_id, WHERE_event_property):
    qCreateEntity = f'''
            MATCH (e:Event) {WHERE_event_property}
            WITH e.{entity_id} AS id
            MERGE (en:Entity {{ID:id, uID:("{entity_type}"+toString(id)), EntityType:"{entity_type}" }})'''
    print(qCreateEntity)
    tx.run(qCreateEntity)
    
def correlate_events_to_entity(tx, entity_type, entity_id, WHERE_event_property):
    qCorrelate = f'''
            MATCH (e:Event) {WHERE_event_property}
            MATCH (n:Entity {{EntityType: "{entity_type}" }}) WHERE e.{entity_id} = n.ID
            CREATE (e)-[:E_EN]->(n)'''
    print(qCorrelate)
    tx.run(qCorrelate)
    
def create_entity_derived_from2(tx, derived_entity_type, entity_type1, entity_type2, fk_2to1):
    qCreateEntity = f'''
            MATCH (e1:Event) -[:E_EN]-> (n1:Entity) WHERE n1.EntityType="{entity_type1}"
            MATCH (e2:Event) -[:E_EN]-> (n2:Entity) WHERE n2.EntityType="{entity_type2}" AND n1 <> n2 AND e2.{fk_2to1} = n1.ID 
            WITH DISTINCT n1.ID as n1_id, n2.ID as n2_id
            WHERE n1_id <> "Unknown" AND n2_id <> "Unknown"
            CREATE ( :Entity {{ {entity_type1}ID: n1_id, {entity_type2}ID: n2_id, EntityType : "{derived_entity_type}", uID :  '{derived_entity_type}_'+toString(n1_id)+'_'+toString(n2_id) }} )'''
    print(qCreateEntity)
    tx.run(qCreateEntity)
    
def correlate_events_to_entity_derived2(tx, derived_entity_type, entity_type1, entity_type2):
    qCorrelate1 = f'''
        MATCH ( e1 : Event ) -[:E_EN]-> (n1:Entity) WHERE n1.EntityType="{entity_type1}"
        MATCH ( derived : Entity ) WHERE derived.EntityType = "{derived_entity_type}" AND n1.ID = derived.{entity_type1}ID
        CREATE ( e1 ) -[:E_EN]-> ( derived )'''
    print(qCorrelate1)
    tx.run(qCorrelate1)
    qCorrelate2 = f'''
        MATCH ( e2 : Event ) -[:E_EN]-> (n2:Entity) WHERE n2.EntityType="{entity_type2}"
        MATCH ( derived : Entity ) WHERE derived.EntityType = "{derived_entity_type}" AND n2.ID = derived.{entity_type2}ID
        CREATE ( e1 ) -[:E_EN]-> ( derived )'''
    print(qCorrelate2)
    tx.run(qCorrelate2)
    
def createDirectlyFollows(tx, entity_type, option_DF_entity_type_in_label):
    qCreateDF = f'''
        MATCH ( n : Entity ) WHERE n.EntityType="{entity_type}"
        MATCH ( n ) <-[:E_EN]- ( e )
        
        WITH n , e as nodes ORDER BY e.timestamp,ID(e)
        WITH n , collect ( nodes ) as nodeList
        UNWIND range(0,size(nodeList)-2) AS i
        WITH n , nodeList[i] as first, nodeList[i+1] as second'''
    qCreateDF = qCreateDF  + '\n'
    
    if option_DF_entity_type_in_label == True:
        qCreateDF = qCreateDF  + f'CREATE ( first ) -[df:DF_{entity_type}]->( second )'
    else:
        qCreateDF = qCreateDF  + f'CREATE ( first ) -[df:DF {{EntityType:n.EntityType}} ]->( second )'

    print(qCreateDF)
    tx.run(qCreateDF)
        
def createEventClass_Activity(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName
        MERGE ( c : Class {{ Name:actName, Type:"Activity", ID: actName}})'''
    print(qCreateEC)
    tx.run(qCreateEC)
        
    qLinkEventToClass = f'''
        MATCH ( e : Event )
        MATCH ( c : Class ) WHERE c.Type = "Activity" AND c.Name = e.Activity
        CREATE ( e ) -[:E_CL]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)
    
def createEventClass_ActivityANDLifeCycle(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName,e.lifecycle AS lifecycle
        MERGE ( c : Class {{ Name:actName+" "+lifecycle, Type:"Activity+Lifecycle", ID: actName+"+"+lifecycle}})'''
    print(qCreateEC)
    tx.run(qCreateEC)
        
    qLinkEventToClass = f'''
        MATCH ( e : Event )
        MATCH ( c : Class ) WHERE c.Type = "Activity+Lifecycle" AND c.ID = e.Activity+"+"+e.lifecycle
        CREATE ( e ) -[:E_CL]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)
    
def createEventClass_Resource(tx):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.resource AS name
        MERGE ( c : Class {{ Name:name, Type:"Resource", ID: name}})'''
    print(qCreateEC)
    tx.run(qCreateEC)
        
    qLinkEventToClass = f'''
        MATCH ( e : Event )
        MATCH ( c : Class ) WHERE c.Type = "Resource" AND c.ID = e.resource
        CREATE ( e ) -[:E_CL]-> ( c )'''
    print(qLinkEventToClass)
    tx.run(qLinkEventToClass)

def aggregateAllDFrelations(tx):
    # most basic aggregation of DF: all DF edges between events of the same classifer between the same entity
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:E_CL]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:E_CL]-> ( c2 : Class )
        MATCH (e1) -[:E_EN] -> (n) <-[:E_EN]- (e2)
        WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType
        WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_CL]-> ( c2 ) ON CREATE SET rel2.count=df_freq,rel2.EntityType=EntityType'''
    print(qCreateDFC)
    tx.run(qCreateDFC)


def aggregateDFrelations(tx, entity_type, event_cl):
    # aggregate only for a specific entity type and event classifier
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:E_CL]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:E_CL]-> ( c2 : Class )
        MATCH (e1) -[:E_EN] -> (n) <-[:E_EN]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_CL]-> ( c2 ) ON CREATE SET rel2.count=df_freq,rel2.EntityType=EntityType'''
    print(qCreateDFC)
    tx.run(qCreateDFC)
    
def aggregateDFrelationsFiltering(tx, entity_type, event_cl, df_threshold, relative_df_threshold):
    # aggregate only for a specific entity type and event classifier
    # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:E_CL]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:E_CL]-> ( c2 : Class )
        MATCH (e1) -[:E_EN] -> (n) <-[:E_EN]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
        WHERE df_freq > {df_threshold}
        OPTIONAL MATCH ( c2 : Class ) <-[:E_CL]- ( e2b : Event ) -[df2:DF]-> ( e1b : Event ) -[:E_CL]-> ( c1 : Class )
        WITH EntityType,c1,df_freq,count(df2) AS df_freq2,c2
        WHERE (df_freq*{relative_df_threshold} > df_freq2)
        MERGE ( c1 ) -[rel2:DF_CL]-> ( c2 ) ON CREATE SET rel2.count=df_freq,rel2.EntityType=EntityType '''
    print(qCreateDFC)
    tx.run(qCreateDFC)
    
    
######################################################
####################### BPIC 17 ######################
######################################################

if step_ClearDB: ### delete all nodes and relations in the graph to start fresh
    print('Clearing DB...')
    qDeleteAllRelations = "MATCH () -[r]- () DELETE r"
    qDeleteAllNodes = "MATCH (n) DELETE n"
    runQuery(driver,qDeleteAllRelations)
    runQuery(driver,qDeleteAllNodes)
    
# table to measure performance
perf = pd.DataFrame(columns=['name', 'start', 'end', 'duration'])
start = time.time()
last = start
    
if step_LoadEventsFromCSV:
    print('Import events from CSV')
    # load CSV tables
    header, csvLog = LoadLog(path_to_neo4j_import_directory+fileName)
    # convert each record in the CSV table into an Event node
    qCreateEvents = CreateEventQuery(header, fileName, 'BPIC17') #generate query to create all events with all log columns as properties
    runQuery(driver, qCreateEvents)

    #create unique constraints
    runQuery(driver, 'CREATE CONSTRAINT ON (e:Event) ASSERT e.ID IS UNIQUE;') #for implementation only (not required by schema or patterns)
    runQuery(driver, 'CREATE CONSTRAINT ON (en:Entity) ASSERT en.uID IS UNIQUE;') #required by core pattern
    runQuery(driver, 'CREATE CONSTRAINT ON (l:Log) ASSERT l.ID IS UNIQUE;') #required by core pattern

    end = time.time()
    perf = perf.append({'name':dataSet+'_event_import', 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
    print('Event nodes done: took '+str(end - last)+' seconds')
    last = end

##create log node and :L_E relationships
if step_createLog:
    with driver.session() as session:
        session.write_transaction(add_log, dataSet)

    end = time.time()
    perf = perf.append({'name':dataSet+'_create_log', 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
    print('Log and :L_E relationships done: took '+str(end - last)+' seconds')
    last = end
    
##create entities
if step_createEntities:
    for entity in model_entities: #per entity
   
        with driver.session() as session:
            session.write_transaction(create_entity, entity[0], entity[1], entity[2])
            print(f'{entity[0]} entity nodes done')
            session.write_transaction(correlate_events_to_entity, entity[0], entity[1], entity[2])
            print(f'{entity[0]} E_EN relationships done')
    
        end = time.time()
        perf = perf.append({'name':dataSet+'_create_entity '+entity[0], 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
        print('Entity '+entity[0]+' done: took '+str(end - last)+' seconds')
        last = end
        
if step_createEntitiesDerived:
    for entity in model_entities_derived: #per entity
   
        with driver.session() as session:
            session.write_transaction(create_entity_derived_from2, entity[0], entity[1], entity[2], entity[3])
            print(f'{entity[0]} entity nodes done')
            session.write_transaction(correlate_events_to_entity_derived2, entity[0], entity[1], entity[2])
            print(f'{entity[0]} E_EN relationships done')
    
        end = time.time()
        perf = perf.append({'name':dataSet+'_create_entity '+entity[0], 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
        print('Entity '+entity[0]+' done: took '+str(end - last)+' seconds')
        last = end

if step_createDF:
    # collect all entities, explicit and derived
    all_entities = []
    for entity in model_entities:
        all_entities.append(entity[0])
    for entity in model_entities_derived:
        all_entities.append(entity[0])
    
    for entity in all_entities: #per entity
        with driver.session() as session:
            session.write_transaction(createDirectlyFollows,entity,option_DF_entity_type_in_label)
            
        end = time.time()
        perf = perf.append({'name':dataSet+'_create_df '+entity, 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
        print('DF for Entity '+entity+' done: took '+str(end - last)+' seconds')
        last = end
        
if step_createEventClasses:
        with driver.session() as session:
            session.write_transaction(createEventClass_Activity)
            session.write_transaction(createEventClass_ActivityANDLifeCycle)
    
        end = time.time()
        perf = perf.append({'name':dataSet+'_create_classes', 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
        print('Event classes done: took '+str(end - last)+' seconds')
        last = end

if step_createDFC:
    all_entities = ["Application", "Workflow", "Offer", "Case_AO", "Case_AW", "Case_WO"]
    for entity in all_entities:
        with driver.session() as session:
            session.write_transaction(aggregateDFrelationsFiltering,entity,"Activity+Lifecycle",10000,3)
            
            
        end = time.time()
        perf = perf.append({'name':dataSet+'_aggregate_df_'+entity, 'start':last, 'end':end, 'duration':(end - last)},ignore_index=True)
        print('Aggregating DF for '+entity+' done: took '+str(end - last)+' seconds')
        last = end

if step_createHOWnetwork:        
    with driver.session() as session:
        session.write_transaction(createEventClass_Resource)
        session.write_transaction(aggregateDFrelations,"Case_AWO","Resource")
            
    end = time.time()
    perf = perf.append({'name':dataSet+'_create_how', 'start':start, 'end':end, 'duration':(end - last)},ignore_index=True)
    print('Creating HOW network done: took '+str(end - last)+' seconds')
    last = end

end = time.time()
perf = perf.append({'name':dataSet+'_total', 'start':start, 'end':end, 'duration':(end - start)},ignore_index=True)
 
perf.to_csv(perfFileName)
driver.close()