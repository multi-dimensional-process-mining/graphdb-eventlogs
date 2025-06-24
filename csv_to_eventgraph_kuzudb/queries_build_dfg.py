import kuzu
import datetime


def runQuery(conn: kuzu.Connection, query: str) -> kuzu.QueryResult:

    start = datetime.datetime.now()
#     print()
#     print(query)
    response = conn.execute(query)
    end = datetime.datetime.now()
    print(str(end-start))

    return response

def createEventClass_ActivityANDLifeCycle(conn: kuzu.Connection):
    qCreateEC = f'''
        MATCH ( e : Event ) WITH distinct e.Activity AS actName,e.lifecycle AS lifecycle
        MERGE ( c : Class {{ Name:actName, Lifecycle:lifecycle, Type:"Activity+Lifecycle", ID: actName+"+"+lifecycle}})'''
    runQuery(conn, qCreateEC)
        
    qLinkEventToClass = f'''
        MATCH ( c : Class ) WHERE c.Type = "Activity+Lifecycle"    
        MATCH ( e : Event ) where e.Activity = c.Name AND e.lifecycle = c.Lifecycle
        CREATE ( e ) -[:OBSERVED]-> ( c )'''
    runQuery(conn, qLinkEventToClass)
    
def aggregateDFrelations(conn: kuzu.Connection, entity_type, event_cl):
    # aggregate only for a specific entity type and event classifier
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
        MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    runQuery(conn, qCreateDFC)
    
def aggregateDFrelationsFiltering(conn: kuzu.Connection, entity_type, event_cl, df_threshold, relative_df_threshold):
    # aggregate only for a specific entity type and event classifier
    # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
    qCreateDFC = f'''
        MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:DF]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
        WHERE n.EntityType = "{entity_type}" AND df.EntityType = "{entity_type}" AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
        WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
        WHERE df_freq > {df_threshold}
        OPTIONAL MATCH ( c2 : Class ) <-[:OBSERVED]- ( e2b : Event ) -[df2:DF]-> ( e1b : Event ) -[:OBSERVED]-> ( c1 : Class )
        WITH EntityType as EType,c1,df_freq,count(df2) AS df_freq2,c2
        WHERE (df_freq*{relative_df_threshold} > df_freq2)
        MERGE ( c1 ) -[rel2:DF_C  {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq'''
    runQuery(conn, qCreateDFC)
    
def prepateDFGtables(conn: kuzu.Connection):
    print("Removing DFG from DB")
    runQuery(conn, "DROP TABLE IF EXISTS OBSERVED")
    runQuery(conn, "DROP TABLE IF EXISTS DF_C")
    runQuery(conn, "DROP TABLE IF EXISTS Class")

    print("Creating tables")
    runQuery(conn, "CREATE NODE TABLE Class (Name STRING, Lifecycle STRING, Type STRING, ID STRING, PRIMARY KEY(ID))")
    runQuery(conn, "CREATE REL TABLE OBSERVED (FROM Event TO Class)")
    runQuery(conn, "CREATE REL TABLE DF_C (FROM Class TO Class, EntityType STRING, count INT32)")
