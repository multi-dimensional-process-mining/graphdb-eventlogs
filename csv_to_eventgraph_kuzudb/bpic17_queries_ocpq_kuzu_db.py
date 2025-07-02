import kuzu
import datetime

def runQuery(conn: kuzu.Connection, query: str) -> kuzu.QueryResult:

    timedeltas = []
    timedeltas_str = []
    for i in range(1,11):
        start = datetime.datetime.now()
        response = conn.execute(query)
        end = datetime.datetime.now()
        timedeltas.append(end-start)
        timedeltas_str.append(str(end-start))

    timedeltas = timedeltas[2:]
    timedeltas_str = timedeltas_str[2:]

    print(timedeltas_str)
    # get average query time
    avg = sum(timedeltas, datetime.timedelta(0)) / len(timedeltas)

    # convert to ms
    avg_ms = avg / datetime.timedelta(milliseconds=1)
    print(str(avg_ms)+" ms")

    return response


def main() -> None:
    # Create an empty on-disk database and connect to it
    db = kuzu.Database("./db_bpic17_ekg_basic")
    conn = kuzu.Connection(db)

    print("Running Q1")

    # original query from https://github.com/aarkue/ocpq-eval/
    #
    # MATCH (o:Entity {EntityType: 'Application'})
    # OPTIONAL MATCH (e:Event) -[:CORR]-> (o)
    # WHERE e.Activity = "A_Submitted"
    # WITH o, COUNT(e) AS eventCount
    # WHERE eventCount <> 1
    # RETURN COUNT(o) AS violationCount

    print("Running Q1 original")
    response = runQuery(conn,'''
        MATCH (o:Entity {EntityType: 'Application'})
        OPTIONAL MATCH (e:Event) -[:CORR]-> (o)
        WHERE e.Activity = "A_Submitted"
        WITH o, COUNT(e) AS eventCount
        WHERE eventCount <> 1
        RETURN COUNT(o) AS violationCount
                            ''')

    while response.has_next():
        print(response.get_next())

    print("Running Q1 modified")
    response = runQuery(conn,'''
        MATCH (o:Entity {EntityType: 'Application'})
        OPTIONAL MATCH (e:Event {activity:"A_Submitted"}) -[:CORR]-> (o)
        WITH o, COUNT(e) AS eventCount
        WHERE eventCount <> 1
        RETURN COUNT(o) AS violationCount
                            ''')
    
    while response.has_next():
        print(response.get_next())


    # print("Running Q2")

    # original query from https://github.com/aarkue/ocpq-eval/
    #
    # MATCH (e:Event)
    # WHERE e.Activity = "O_Created"
    # OPTIONAL MATCH (e) -[:DF*{EntityType: 'Offer'}]-> (e2:Event)
    # WHERE e2.Activity = "O_Returned"
    # WITH e, COUNT(e2) AS eventCount
    # WHERE eventCount < 1
    # RETURN COUNT(e) AS violationCount

    print("Running Q2 original")
    response = runQuery(conn,'''
        MATCH (e:Event {activity: "O_Created"})
        WHERE NOT EXISTS {
            MATCH (e) -[:DF* SHORTEST {EntityType: 'Offer'}]-> (:Event{activity: "O_Returned"})
        }        
        RETURN COUNT(e) AS violationCount
                            ''')

    while response.has_next():
        print(response.get_next())

    print("Running Q2 modified")
    response = runQuery(conn,'''
        MATCH (e:Event {activity: "O_Created"})-[:CORR]->(o:Entity {EntityType: 'Offer'})
        WHERE NOT EXISTS {
            MATCH (o) <-[:CORR]- (e2:Event{activity: "O_Returned"}) WHERE e.timestamp < e2.timestamp
        }        
        RETURN COUNT(e) AS violationCount
                            ''')

    while response.has_next():
        print(response.get_next())

    
    print("Running Q3")

    # original query from https://github.com/aarkue/ocpq-eval/
    #
    # MATCH (e:Event)
    # WHERE e.Activity = "O_Returned"
    # OPTIONAL MATCH (e) -[:CORR]->  (o:Entity {EntityType: 'Offer'})
    # WITH e, COUNT(o) AS offerCount
    # WHERE offerCount <> 1
    # RETURN COUNT(e) AS violationCount

    print("Running Q3 original")
    response = runQuery(conn,'''
        MATCH (e:Event)
        WHERE e.Activity = "O_Returned"
        OPTIONAL MATCH (e) -[:CORR]-> (o:Entity {EntityType: 'Offer'})
        WITH e, COUNT(o) AS offerCount
        WHERE offerCount <> 1
        RETURN COUNT(e) AS violationCount
                            ''')

    while response.has_next():
        print(response.get_next())

    print("Running Q3 modified")
    response = runQuery(conn,'''
        MATCH (e:Event {activity: "O_Returned"})
        OPTIONAL MATCH (e) -[:CORR]->  (o:Entity {EntityType: 'Offer'})
        WITH e, COUNT(o) AS offerCount
        WHERE offerCount <> 1
        RETURN COUNT(e) AS violationCount
                            ''')
    
    while response.has_next():
        print(response.get_next())


    print("Running Q4")

    # original query from https://github.com/aarkue/ocpq-eval/
    #
    # MATCH (o1:Entity { EntityType: "Application" }) <-[:CORR]- (e1:Event {Activity: "A_Accepted"})
    # OPTIONAL MATCH (e2:Event { Activity: "O_Accepted" }) -[:CORR]-> (o2:Entity {EntityType: "Offer"}) -[:REL]-> (o1)
    # WHERE e1.timestamp <= e2.timestamp
    # WITH o1, COUNT(e2) AS e2_c, COUNT(o2) AS o2_c
    # WHERE e2_c < 1 OR o2_c < 1
    # RETURN COUNT(o1) AS violationCount

    print("Running Q4 original")
    response = runQuery(conn,'''
        MATCH (o1:Entity { EntityType: "Application" }) <-[:CORR]- (e1:Event {activity:"A_Accepted"})
        OPTIONAL MATCH (e2:Event { Activity: "O_Accepted" }) -[:CORR]-> (o2:Entity {EntityType: "Offer"}) -[:REL]-> (o1)
        WHERE e1.timestamp <= e2.timestamp
        WITH o1, COUNT(e2) AS e2_c, COUNT(o2) AS o2_c
        WHERE e2_c < 1 OR o2_c < 1
        RETURN COUNT(o1) AS violationCount
                            ''')
    
    while response.has_next():
        print(response.get_next())

    print("Running Q4 modified")
    response = runQuery(conn,'''
        MATCH (o1:Entity { EntityType: "Application" }) <-[:CORR]- (e1:Event {activity:"A_Accepted"})
        WHERE NOT EXISTS {
            MATCH (o1) <-[:REL]- (:Entity {EntityType: "Offer"}) <-[:CORR]- (e2:Event {activity:"O_Accepted"}) WHERE e1.timestamp <= e2.timestamp
        }
        RETURN COUNT(o1) as violationCount
                            ''')
    
    while response.has_next():
        print(response.get_next())


    print("Running Q5")

    # original query from https://github.com/aarkue/ocpq-eval/
    #
    # MATCH (o1:Entity {EntityType: 'Application'}) <-[:CORR]- (e1:Event {Activity: 'A_Accepted'}) -[:CORR]-> (o2:Entity {EntityType: 'Case_R'})
    # OPTIONAL MATCH (e2:Event {Activity: 'O_Created'}) -[:CORR]-> (o3:Entity {EntityType: 'Offer'}) -[:REL]-> (o1)
    # WHERE NOT (e2) -[:CORR]-> (o2)
    # WITH o1, COUNT(e2) as e2_c, COUNT(o3) AS o3_c
    # WHERE e2_c >= 1 OR o3_c >= 1
    # RETURN COUNT(*) AS violationCount

    print("Running Q5 original")
    response = runQuery(conn,'''
        MATCH (o1:Entity {EntityType: 'Application'}) <-[:CORR]- (e1:Event {Activity: 'A_Accepted'}) -[:CORR]-> (o2:Entity {EntityType: 'Resource'})
        OPTIONAL MATCH (e2:Event {Activity: 'O_Created'}) -[:CORR]-> (o3:Entity {EntityType: 'Offer'}) -[:REL]-> (o1)
        WHERE NOT (e2) -[:CORR]-> (o2)
        WITH o1, COUNT(e2) as e2_c, COUNT(o3) AS o3_c
        WHERE e2_c >= 1 OR o3_c >= 1
        RETURN COUNT(*) AS violationCount
                            ''')
    
    while response.has_next():
        print(response.get_next())

    print("Running Q5 modified")
    response = runQuery(conn,'''
        MATCH (o1:Entity {EntityType: 'Application'}) <-[:CORR]- (e1:Event {activity: 'A_Accepted'}) -[:CORR]-> (o2: Entity {EntityType: 'Resource'})
        OPTIONAL MATCH (o1) <-[:REL]- (o3: Entity {EntityType: 'Offer'}) <-[:CORR]- (e2:Event {activity: 'O_Created'}) -[:CORR]-> (o4:Entity {EntityType: 'Resource'}) WHERE o2 <> o4 
        WITH o1, COUNT(e2) as e2_c
        WHERE e2_c >= 1
        RETURN COUNT(o1) AS violationCount
                            ''')

    while response.has_next():
        print(response.get_next())

    # response = runQuery(conn,'''
    #     MATCH (o1:Entity {EntityType: 'Application'}) <-[:CORR]- (e1:Event {activity: 'A_Accepted'}) -[:CORR]-> (o2: Entity {EntityType: 'Resource'})
    #     WHERE EXISTS { 
    #         MATCH (o1) <-[:REL]- (o3: Entity {EntityType: 'Offer'}) <-[:CORR]- (e2:Event {activity: 'O_Created'}) -[:CORR]-> (o4:Entity {EntityType: 'Resource'}) WHERE o2 <> o4 
    #     }
    #     RETURN COUNT(o1) AS violationCount
    #         ''')

    # while response.has_next():
    #     print(response.get_next())

    print("Running Q5 modified #2")
    response = runQuery(conn,'''
        MATCH (o1:Entity {EntityType: 'Application'}) <-[:CORR]- (e1:Event {activity: 'A_Accepted'})
        OPTIONAL MATCH (o1) <-[:REL]- (o3: Entity {EntityType: 'Offer'}) <-[:CORR]- (e2:Event {activity: 'O_Created'}) WHERE e1.resource <> e2.resource 
        WITH o1, COUNT(e2) as e2_c
        WHERE e2_c >= 1
        RETURN COUNT(o1) AS violationCount
                            ''')

    while response.has_next():
        print(response.get_next())

    print("Running Q6")

    # original query from https://github.com/aarkue/ocpq-eval/
    #
    # MATCH (e1:Event) −[:CORR] −> (o: Entity {EntityType : 'Offer'}) <−[:CORR] − (e2:Event)
    # WHERE e1.Activity = "O_Created" and e2.Activity = "O_Accepted"
    # WITH duration.between(e1.timestamp,e2.timestamp) AS time, e1.timestamp as t1, e2.timestamp as t2
    # ORDER BY time DESC
    # LIMIT 1
    # RETURN duration.inSeconds(t1,t2).seconds

    response = runQuery(conn,'''
        MATCH (e1:Event {activity : "O_Created"}) -[:CORR] -> (o:Entity {EntityType : 'Offer'}) <-[:CORR]- (e2:Event {activity : "O_Accepted"})
        WITH e2.timestamp - e1.timestamp AS time ORDER BY time DESC
        LIMIT 1
        RETURN time
                            ''')
    
    while response.has_next():
        print(response.get_next())

    print("Running Q7")

    # original query from https://github.com/aarkue/ocpq-eval/
    #
    # MATCH (e2:Event {Activity: 'O_Created'}) -[:CORR]-> (o2:Entity {EntityType: 'Offer'}) -[:REL]-> (o1:Entity {EntityType: 'Application'})
    # MATCH (o1)  <-[:REL]- (o3:Entity {EntityType: 'Offer'}) <-[:CORR]- (e3:Event {Activity: 'O_Created'})
    # RETURN COUNT(*)

    print("Running Q7 original")
    response = runQuery(conn,'''
        MATCH (e2:Event {Activity: 'O_Created'}) -[:CORR]-> (o2:Entity {EntityType: 'Offer'}) -[:REL]-> (o1:Entity {EntityType: 'Application'})
        MATCH (o1)  <-[:REL]- (o3:Entity {EntityType: 'Offer'}) <-[:CORR]- (e3:Event {Activity: 'O_Created'})
        RETURN COUNT(*)
                            ''')
    
    while response.has_next():
        print(response.get_next())

    print("Running Q7 modified")
    response = runQuery(conn,'''
        MATCH (e2:Event {activity: 'O_Created'}) -[:CORR]-> (o2:Entity {EntityType: 'Offer'}) -[:REL]-> (o1:Entity {EntityType: 'Application'}) <-[:REL]- (o3:Entity {EntityType: 'Offer'}) <-[:CORR]- (e3:Event {activity: 'O_Created'})
        RETURN COUNT(*)
                            ''')
    
    while response.has_next():
        print(response.get_next())

    print("Running Q7 modified #2")
    response = runQuery(conn,'''
        MATCH (o2:Entity {EntityType: 'Offer'}) -[:REL]-> (o1:Entity {EntityType: 'Application'}) <-[:REL]- (o3:Entity {EntityType: 'Offer'})
        RETURN COUNT(*)
                            ''')
    
    while response.has_next():
        print(response.get_next())


    print("Running Q7 modified to check for applications with distinct offers")
    response = runQuery(conn,'''
        MATCH (e2:Event {Activity: 'O_Created'}) -[:CORR]-> (o2:Entity {EntityType: 'Offer'}) -[:REL]-> (o1:Entity {EntityType: 'Application'})
        MATCH (o1)  <-[:REL]- (o3:Entity {EntityType: 'Offer'}) <-[:CORR]- (e3:Event {Activity: 'O_Created'})
        WHERE o1 <> o3
        RETURN COUNT(*)
                            ''')
    
    while response.has_next():
        print(response.get_next())


    response = runQuery(conn,'''
        MATCH (e2:Event {activity: 'O_Created'}) -[:CORR]-> (o2:Entity {EntityType: 'Offer'}) -[:REL]-> (o1:Entity {EntityType: 'Application'}) 
        MATCH (o1) <-[:REL]- (o3:Entity {EntityType: 'Offer'}) <-[:CORR]- (e3:Event {activity: 'O_Created'})
        WHERE o2 <> o3
        RETURN COUNT(*)
                            ''')
    
    while response.has_next():
        print(response.get_next())

    response = runQuery(conn,'''
        MATCH (o2:Entity {EntityType: 'Offer'}) -[:REL]-> (o1:Entity {EntityType: 'Application'})
        MATCH (o1) <-[:REL]- (o3:Entity {EntityType: 'Offer'})
        WHERE o2 <> o3
        RETURN COUNT(*)
                            ''')
    
    while response.has_next():
        print(response.get_next())

main()