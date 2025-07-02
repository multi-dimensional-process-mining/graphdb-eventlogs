import kuzu
import csv
import datetime
import infer_df_edges_typed
import queries_build_dfg_typed

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
db = kuzu.Database("./db_bpic17_ekg_typed")
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
    print()
    print(query)
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
            MERGE (en:{entity_type} {{ID:id, uID:("{entity_type}"+id), EntityType:"{entity_type}" }})'''
    runQuery(qCreateEntity)

def correlate_events_to_entity(entity_type, entity_id, WHERE_event_property):
    qCorrelate = f'''
            MATCH (e:Event) {WHERE_event_property}
            MATCH (n:{entity_type} {{EntityType: "{entity_type}" }}) WHERE e.{entity_id} = n.ID
            CREATE (e)-[:CORR]->(n)'''
    runQuery(qCorrelate)

def create_entity_relationships(relation_type, entity_type1, entity_type2, reference_from1to2):
    qCreateRelation = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:{entity_type1} )
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:{entity_type2} )
                WHERE n1 <> n2 AND e2.{reference_from1to2} = n1.ID
            WITH DISTINCT n1,n2
            CREATE ( n1 ) <-[:REL {{Type:"{relation_type}"}} ]- ( n2 )'''
    runQuery(qCreateRelation)

def reify_entity_relations(relation_type, to_entity, from_entity):
    qReifyRelation = f'''
            MATCH ( n1:{from_entity} ) -[:REL {{Type:"{relation_type}"}}]-> ( n2:{to_entity} )
            CREATE (n1) <-[:DERIVED]- (new : {relation_type} {{ 
                ID:n1.ID+"_"+n2.ID,
                EntityType: "{relation_type}",
                uID:"{relation_type}"+n1.ID+"_"+n2.ID }} )
                -[:DERIVED]-> (n2)'''
    runQuery(qReifyRelation)

def correlate_events_to_derived_entity(derived_entity_type):
    qCorrelate = f'''
            MATCH (e:Event) -[:CORR]-> (n) <-[:DERIVED]- (r:{derived_entity_type} )
            CREATE (e)-[:CORR]->(r)'''
    runQuery(qCorrelate)


print("Clearing database")
runQuery("DROP TABLE IF EXISTS DF_C")
runQuery("DROP TABLE IF EXISTS OBSERVED")
runQuery("DROP TABLE IF EXISTS CORR")
runQuery("DROP TABLE IF EXISTS DF")
runQuery("DROP TABLE IF EXISTS DERIVED")
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


# build table for Entitys and CORR relations
print("Building typed node and relation tables")

corrString = ""     # build type string for CORR relation (Event to Entity)
for entity in model_entities: #per entity

    entity_type = entity[0]

    if entity_type in include_entities:
        runQuery(f"DROP TABLE IF EXISTS {entity_type}")
        runQuery(f"CREATE NODE TABLE {entity_type} (ID STRING, EntityType STRING, uID STRING, PRIMARY KEY(uID))")
        corrString = corrString + f"FROM Event TO {entity_type}, "


# build table for relations between entities
# also consider entities derived from relations
relString = ""         # build type string for REL relation (Entity to Entity) 
derivedString = ""     # build type string for DERIVED relation (Entity to Entity)
for relation in model_relations: #per relation

    derived_entity = relation[0]
    to_entity = relation[1]
    from_entity = relation[2]

    # extend REL table: from Entity to Entity
    relString = relString + f"FROM {from_entity} TO {to_entity}, "      

    if derived_entity in model_entities_derived and derived_entity in include_entities: # relations to materialize as entity

        # create node table
        runQuery(f"DROP TABLE IF EXISTS {derived_entity}")
        runQuery(f"CREATE NODE TABLE {derived_entity} (ID STRING, EntityType STRING, uID STRING, PRIMARY KEY(uID))")

        # allow derived entity to be correlated to events
        corrString = corrString + f"FROM Event TO {derived_entity}, "

        # extend DERIVED table: from derived to from/to entity of the relation
        derivedString = derivedString + f"FROM {derived_entity} TO {from_entity}, "
        derivedString = derivedString + f"FROM {derived_entity} TO {to_entity}, "

# build CORR table from type string
corrString = corrString[:-2]
runQuery(f"CREATE REL TABLE CORR ("+corrString+")")

# build REL table from type string
relString = relString + "Type STRING"
runQuery("CREATE REL TABLE REL ("+relString+")")

# build DERVIED table from type string
derivedString = derivedString[:-2]
runQuery("CREATE REL TABLE DERIVED ("+derivedString+")")


# infer entity nodes (basic)
print("Inferring Entities")

for entity in model_entities: #per entity

    if entity[0] in include_entities:

        create_entity(entity[0], entity[1], entity[2])
        correlate_events_to_entity(entity[0], entity[1], entity[2])
        print(f'{entity[0]} entity nodes done')

        response = conn.execute(f"MATCH (e:{entity[0]}) RETURN count(e)")
        while response.has_next():
            print(response.get_next())

# infer relations between entities
print("Inferring Relations")

for relation in model_relations: #per relation
    create_entity_relationships(relation[0], relation[1], relation[2], relation[3])
    print(f'{relation[0]} relationships created')

# reify selected relations into entities

for relation in model_relations: #per relation
    derived_entity = relation[0]
    to_entity = relation[1]
    from_entity = relation[2]

    if derived_entity in model_entities_derived and derived_entity in include_entities:

        reify_entity_relations(derived_entity, to_entity, from_entity)
        print(f'{derived_entity} relationships reified')
        correlate_events_to_derived_entity(derived_entity)
        print(f'{derived_entity} CORR relationships created')

endBuildEKG = datetime.datetime.now()


# infer DF edges
startInferDF = datetime.datetime.now()
infer_df_edges_typed.infer_df(conn, False)
endInferDF = datetime.datetime.now()


# build DFG
startBuildDFG = datetime.datetime.now()

queries_build_dfg_typed.prepareDFGtables(conn)

print("Aggregate Activities")
queries_build_dfg_typed.createEventClass_ActivityANDLifeCycle(conn)

print("Aggregate DF edges")
classifier = "Activity+Lifecycle"
for entity in include_entities:
    queries_build_dfg_typed.aggregateDFrelations(conn, entity, classifier)
    #queries_build_dfg.aggregateDFrelationsFiltering(conn, entity, classifier, 500, 3)

endBuildDFG = datetime.datetime.now()

conn.close()

print("Build EKG: "+str(endBuildEKG-startBuildEKG))
print("Infer DF: "+str(endInferDF-startInferDF))
print("Build DFG: "+str(endBuildDFG-startBuildDFG))
