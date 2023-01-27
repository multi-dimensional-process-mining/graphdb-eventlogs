from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

#match (e:Event),(n:Entity),(c:Class),(l:Log),()-[df:DF]->(),()-[corr:E_EN]->() return count(e),count(n),count(c),count(l),count(df),count(corr)

def get_size(tx):
    num_events, num_entity, num_log, num_class, num_df, num_corr, num_rel, num_total_nodes, num_total_relationships\
        = 0, 0, 0, 0, 0, 0, 0, 0, 0
    q = f'''match (e:Event) RETURN count(e)'''
    for record in tx.run(q):
        num_events = record["count(e)"]

    q = f'''match (e:Entity) RETURN count(e)'''
    for record in tx.run(q):
        num_entity = record["count(e)"]

    q = f'''match (l:Log) RETURN count(l)'''
    for record in tx.run(q):
        num_log = record["count(l)"]

    q = f'''match (c:Class) RETURN count(c)'''
    for record in tx.run(q):
        num_class = record["count(c)"]

    q = f'''match ()-[df:DF]->() RETURN count(df)'''
    for record in tx.run(q):
        num_df = record["count(df)"]

    q = f'''match ()-[corr:E_EN]->() RETURN count(corr)'''
    for record in tx.run(q):
        num_corr = record["count(corr)"]

    q = f'''match ()-[r:REL]->() RETURN count(r)'''
    for record in tx.run(q):
        num_rel = record["count(r)"]

    q = f'''match (n) RETURN count(n)'''
    for record in tx.run(q):
        num_total_nodes = record["count(n)"]

    q = f'''match ()-[r]->() RETURN count(r)'''
    for record in tx.run(q):
        num_total_relationships = record["count(r)"]

    print(f'''Nodes (total);Event;Entity;Log;Class;Relationships (total);DF;CORR;REL''')
    print(f'''{num_total_nodes};{num_events};{num_entity};{num_log};{num_class};{num_total_relationships};{num_df};
    {num_corr};{num_rel}''')



with driver.session() as session:
    session.read_transaction(get_size)
