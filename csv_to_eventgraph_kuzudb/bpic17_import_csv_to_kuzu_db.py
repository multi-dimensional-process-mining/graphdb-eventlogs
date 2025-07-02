import kuzu
import csv
import datetime
import infer_df_edges
import queries_build_dfg

#inputCSV = "prepared/BPIC17sample.csv"
inputCSV = "prepared/BPIC17full.csv"

# specification of the data transformation
log_name = "BPIC17"

include_entities = ['Application','Workflow','Offer','Resource','Case_AO','Case_AW','Case_WO']
#include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO','Case_AWO']


model_entities = [['Application','ApplicationId', 'WHERE e.EventOrigin = "Application"'], # individual entities
                  ['Workflow', 'ApplicationId', 'WHERE e.EventOrigin = "Workflow"'],
                  ['Offer', 'OfferID', 'WHERE e.EventOrigin = "Offer"'],
                  ['Resource', 'resource', 'WHERE e.resource IS NOT NULL'], # resource as entity
                  ['Case_AWO','ApplicationId', 'WHERE e.ApplicationId IS NOT NULL']] # original case notion

# specification of relations between entities
#    1 name of the relation
#    2 name of first entity, 
#    3 name of second entity where events have an property referring to the first entity, i.e., a foreign key
#    4 name of the foreign key property by which events of the second entity refer to the first entity
model_relations = [['Case_AO','Application','Offer','ApplicationId'],
                   ['Case_AW','Application','Workflow','ApplicationId'],
                   ['Case_WO','Workflow','Offer','ApplicationId']]

# specification of entities to derive by reifying relations: 
#    1 name of the relation in 'model_relations' that shall be reified
model_entities_derived = ['Case_AO',
                          'Case_AW',
                          'Case_WO']


# Create an empty on-disk database and connect to it
db = kuzu.Database("./db_bpic17_ekg_basic")
conn = kuzu.Connection(db)

# load log header (attribute names) from import file
def getLogHeader(fileName):
    with open(fileName) as f:
        reader = csv.reader(f)
        logHeader = list(next(reader))
        f.close()
    return logHeader

def runQuery(query: str) -> kuzu.QueryResult:

    start = datetime.datetime.now()
    #print()
    #print(query)
    response = conn.execute(query)
    end = datetime.datetime.now()
    print(str(end-start))

    return response

def add_log(log_id):
    qCreateLog = f'CREATE (:Log {{ID: "{log_id}" }})'
    runQuery(qCreateLog)

    qLinkEventsToLog = f'''
            MATCH (e:Event {{Log: "{log_id}" }}) 
            MATCH (l:Log {{ID: "{log_id}" }}) 
            CREATE (l)-[:HAS]->(e)'''
    runQuery(qLinkEventsToLog)

def create_entity(entity_type, entity_id, WHERE_event_property):
    qCreateEntity = f'''
            MATCH (e:Event) {WHERE_event_property}
            WITH e.{entity_id} AS id
            MERGE (en:Entity {{ID:id, uID:("{entity_type}"+id), EntityType:"{entity_type}" }})'''
    runQuery(qCreateEntity)

def correlate_events_to_entity(entity_type, entity_id, WHERE_event_property):
    qCorrelate = f'''
            MATCH (e:Event) {WHERE_event_property}
            MATCH (n:Entity {{EntityType: "{entity_type}" }}) WHERE e.{entity_id} = n.ID
            CREATE (e)-[:CORR]->(n)'''
    runQuery(qCorrelate)

def create_entity_relationships(relation_type, entity_type1, entity_type2, reference_from1to2):
    qCreateRelation = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:Entity ) WHERE n1.EntityType="{entity_type1}"
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:Entity ) WHERE n2.EntityType="{entity_type2}"
                AND n1 <> n2 AND e2.{reference_from1to2} = n1.ID
            WITH DISTINCT n1,n2
            CREATE ( n1 ) <-[:REL {{Type:"{relation_type}"}} ]- ( n2 )'''
    runQuery(qCreateRelation)

def reify_entity_relations(relation_type):
    qReifyRelation = f'''
            MATCH ( n1 : Entity ) -[rel:REL {{Type:"{relation_type}"}}]-> ( n2:Entity )
            CREATE (n1) <-[:REL {{Type:"Reified"}}]- (new : Entity {{ 
                ID:n1.ID+"_"+n2.ID,
                EntityType: "{relation_type}",
                uID:"{relation_type}"+n1.ID+"_"+n2.ID }} )
                -[:REL {{Type:"Reified"}}]-> (n2)'''
    runQuery(qReifyRelation)

def correlate_events_to_derived_entity(derived_entity_type):
    qCorrelate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REL {{Type:"Reified"}}]- (r:Entity {{EntityType:"{derived_entity_type}"}} )
            CREATE (e)-[:CORR]->(r)'''
    runQuery(qCorrelate)


print("Clearing database")
runQuery("DROP TABLE IF EXISTS DF_C")
runQuery("DROP TABLE IF EXISTS OBSERVED")
runQuery("DROP TABLE IF EXISTS CORR")
runQuery("DROP TABLE IF EXISTS DF")
runQuery("DROP TABLE IF EXISTS REL")
runQuery("DROP TABLE IF EXISTS HAS")
runQuery("DROP TABLE IF EXISTS Event")
runQuery("DROP TABLE IF EXISTS Entity")
runQuery("DROP TABLE IF EXISTS Class")
runQuery("DROP TABLE IF EXISTS Log")

startBuildEKG = datetime.datetime.now()

# build basic data definition string for importing event table
header = getLogHeader(inputCSV)
ddlString = ""
for col in header:
    if col in ['timestamp','start','end']:
        ddlEntry = f'{col} TIMESTAMP, '
    elif col in ['idx']:
        ddlEntry = f'{col} INT32, '
    else:
        ddlEntry = f'{col} STRING, '

    ddlString = ddlString + ddlEntry
ddlString = ddlString + "PRIMARY KEY(idx)"

runQuery("CREATE NODE TABLE Event ("+ddlString+")")

# importing events
print("Importing Events")
runQuery("COPY Event FROM '"+inputCSV+"' (header=true, quote='\"');")

# extend events with "Log" property, set to log_name
runQuery("ALTER TABLE Event ADD Log STRING DEFAULT '"+log_name+"'")

response = runQuery("MATCH (e:Event) RETURN count(e)")
while response.has_next():
    print(response.get_next())

# link to log node
runQuery("CREATE NODE TABLE Log (ID STRING, PRIMARY KEY(ID))")
runQuery("CREATE REL TABLE HAS (FROM Log TO Event)")
add_log(log_name)

# infer entity nodes (basic)
print("Inferring Entities")
runQuery("CREATE NODE TABLE Entity (ID STRING, EntityType STRING, uID STRING, PRIMARY KEY(uID))")
runQuery("CREATE REL TABLE CORR (FROM Event TO Entity)")

for entity in model_entities: #per entity
    if entity[0] in include_entities:
        create_entity(entity[0], entity[1], entity[2])
        correlate_events_to_entity(entity[0], entity[1], entity[2])
        print(f'{entity[0]} entity nodes done')

response = conn.execute("MATCH (e:Entity) RETURN count(e)")
while response.has_next():
    print(response.get_next())

# infer relations between entities
print("Inferring Relations")
runQuery("CREATE REL TABLE REL (FROM Entity TO Entity, Type STRING)")

for relation in model_relations: #per relation
    create_entity_relationships(relation[0], relation[1], relation[2], relation[3])
    print(f'{relation[0]} relationships created')

# reify selected relations into entities
for relation in model_relations: #per relation
    derived_entity = relation[0]
    if derived_entity in model_entities_derived and derived_entity in include_entities:
        reify_entity_relations(derived_entity)
        print(f'{derived_entity} relationships reified')
        correlate_events_to_derived_entity(derived_entity)
        print(f'{derived_entity} CORR relationships created')

endBuildEKG = datetime.datetime.now()


# infer DF edges
startInferDF = datetime.datetime.now()
infer_df_edges.infer_df(conn, False)
endInferDF = datetime.datetime.now()


# build DFG
startBuildDFG = datetime.datetime.now()

queries_build_dfg.prepareDFGtables(conn)

print("Aggregate Activities")
queries_build_dfg.createEventClass_ActivityANDLifeCycle(conn)

print("Aggregate DF edges")
classifier = "Activity+Lifecycle"
for entity in include_entities:
    queries_build_dfg.aggregateDFrelations(conn, entity, classifier)
    #queries_build_dfg.aggregateDFrelationsFiltering(conn, entity, classifier, 500, 3)

endBuildDFG = datetime.datetime.now()

conn.close()

print("Build EKG: "+str(endBuildEKG-startBuildEKG))
print("Infer DF: "+str(endInferDF-startInferDF))
print("Build DFG: "+str(endBuildDFG-startBuildDFG))
