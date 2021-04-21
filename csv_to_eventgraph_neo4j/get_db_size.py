from neo4j import GraphDatabase

driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))

#match (e:Event),(n:Entity),(c:Class),(l:Log),()-[df:DF]->(),()-[corr:E_EN]->() return count(e),count(n),count(c),count(l),count(df),count(corr)

def getSize(tx):

    q = f'''match (e:Event) RETURN count(e)'''
    for record in tx.run(q):
        numEvents = record["count(e)"]

    q = f'''match (e:Entity) RETURN count(e)'''
    for record in tx.run(q):
        numEntity = record["count(e)"]

    q = f'''match (l:Log) RETURN count(l)'''
    for record in tx.run(q):
        numLog = record["count(l)"]

    q = f'''match (c:Class) RETURN count(c)'''
    for record in tx.run(q):
        numClass = record["count(c)"]

    q = f'''match ()-[df:DF]->() RETURN count(df)'''
    for record in tx.run(q):
        numDF = record["count(df)"]

    q = f'''match ()-[corr:E_EN]->() RETURN count(corr)'''
    for record in tx.run(q):
        numCorr = record["count(corr)"]

    q = f'''match ()-[r:REL]->() RETURN count(r)'''
    for record in tx.run(q):
        numRel = record["count(r)"]

    q = f'''match (n) RETURN count(n)'''
    for record in tx.run(q):
        numTotalNodes = record["count(n)"]

    q = f'''match ()-[r]->() RETURN count(r)'''
    for record in tx.run(q):
        numTotalRelationShips = record["count(r)"]

    print(f'''Nodes (total);Event;Entity;Log;Class;Relationships (total);DF;CORR;REL''')
    print(f'''{numTotalNodes};{numEvents};{numEntity};{numLog};{numClass};{numTotalRelationShips};{numDF};{numCorr};{numRel}''')



with driver.session() as session:
    session.read_transaction(getSize)
