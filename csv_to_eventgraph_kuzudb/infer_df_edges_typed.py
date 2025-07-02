import kuzu
import datetime
import pandas as df
import numpy as np

def runQuery(conn: kuzu.Connection, query: str) -> kuzu.QueryResult:

    start = datetime.datetime.now()
    # print()
    # print(query)
    response = conn.execute(query)
    end = datetime.datetime.now()
    print(str(end-start))

    return response

def getEntityTypes(conn: kuzu.Connection):
    qGetEntityTypes = f'''
        MATCH ( e : Event ) -[:CORR]-> ( n ) RETURN DISTINCT LABEL(n) AS label
        '''
    et_df = runQuery(conn, qGetEntityTypes).get_as_df()
    return et_df["label"].to_list()

def createDirectlyFollowsFast(conn: kuzu.Connection, entity_type):

    start = datetime.datetime.now()

    # query for all events correlated to an entity, ordered by time
    # uID,ID,type of entity + idx of event which will be the source event for the DF edge
    qEntityEventsOrdered = f'''
        MATCH ( n : {entity_type} )
        MATCH ( n ) <-[:CORR]- ( e : Event )
        RETURN n.uID AS uID, n.ID AS ID, n.EntityType AS EntityType, e.idx AS src ORDER BY n.uID, e.timestamp ASC, e.idx ASC
        '''
    result = runQuery(conn, qEntityEventsOrdered)
    corr_sorted = result.get_as_df()

    #print(corr_sorted)

    # in corr_sorted["src"], the event at index i+1 directly follows 
    # the event at index i (if i and i+1 are both related to the same entity uID)

    # make copy of index of src event index, shift up by 1
    # tgt_event_idx[i] now holds the successor event of corr_sorted["src"][i]
    tgt_event_idx = corr_sorted["src"].to_list()[1:]
    tgt_event_idx.append(-1) # pad to have list of equal length
    # do the same for entity uID
    # tgt_entityuID[i] now holds the entity of the successor of corr_sorted["src"][i]
    tgt_entity_uID = corr_sorted["uID"].to_list()[1:]
    tgt_entity_uID.append("-")


    # build table to import records as DF edges later on
    # structure: primary key of src node, primary key of target node, [DF edge property]*
    # build table from existing dataframe
    df_edges = corr_sorted[["src","ID","EntityType","uID"]]
    # insert column of target nodes
    df_edges.insert(1,"tgt",tgt_event_idx, True)
    # insert column of target entity uID
    df_edges.insert(5,"uID_tgt",tgt_entity_uID, True)
    # now every record in df_edges holds
    # df_edges["src"] is event index of source event
    # df_edges["tgt"] is event index of successor/target event
    # df_edges["uID"] is entity of source event
    # df_edges["uID_tgt"] is entity of successor/target event

    # remove all records where uID != uID_tgt as they relate events of different entities
    # the remaining records hold: event index + successor event index all related the same entity
    df = df_edges.loc[np.where(df_edges["uID"].values==df_edges["uID_tgt"].values)]
    # drop auxiliary columns of uIDs
    df.drop(['uID','uID_tgt'], axis=1, inplace=True)
    #print(df)

    # import edges directly into DF table
    conn.execute("COPY DF FROM df")
    end = datetime.datetime.now()
    print(str(end-start))

def getDerivedEntityTypes(conn: kuzu.Connection):
    qGetDerivedEntityTypes = f'''
        MATCH ( n ) -[:DERIVED]-> ( n2 )
        RETURN DISTINCT LABEL(n), LABEL(n2)
        '''
    res = runQuery(conn, qGetDerivedEntityTypes)
    et_pairs = []
    while res.has_next():
        et_pairs.append(res.get_next())
    return et_pairs


def deleteParallelDirectlyFollows_Derived(conn: kuzu.Connection, derived_entity_type, original_entity_type):
    qDeleteDF = f'''
        MATCH (e1:Event) -[df:DF {{EntityType: "{derived_entity_type}" }}]-> (e2:Event)
        WHERE (e1:Event) -[:DF {{EntityType: "{original_entity_type}" }}]-> (e2:Event)
        DELETE df'''
    runQuery(conn, qDeleteDF)

def infer_df(conn: kuzu.Connection, delete_parallel_df: bool = True):

    print("Removing DF from DB")
    runQuery(conn, "DROP TABLE IF EXISTS DF")

    # infer directly-follows
    print("Inferring DF")
    runQuery(conn, "CREATE REL TABLE DF (FROM Event TO Event, ID STRING, EntityType STRING)")


    entities = getEntityTypes(conn)
    for entity in entities:
        createDirectlyFollowsFast(conn, entity)
        print(f'{entity} df done')

    res = runQuery(conn, "MATCH ()-[df:DF]->() RETURN count(df)")
    while res.has_next():
        print(res.get_next())

    # delete parallel DF relations of derived entities
    if delete_parallel_df:
        et_derived_pairs = getDerivedEntityTypes(conn)
        for et_derived_pair in et_derived_pairs: #for each derived entity and one of its contributing entities

            # delete df relations of derived entity when in parallel with  contributing entity
            deleteParallelDirectlyFollows_Derived(conn, et_derived_pair[0], et_derived_pair[1])
            print(f'{et_derived_pair[0]} cleaned df wrt. {et_derived_pair[1]}')

