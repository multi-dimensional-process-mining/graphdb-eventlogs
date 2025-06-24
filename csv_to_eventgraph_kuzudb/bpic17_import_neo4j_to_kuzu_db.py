import kuzu

def main() -> None:
    # Create an empty on-disk database and connect to it
    db = kuzu.Database("./db_bpic17_ekg")
    conn = kuzu.Connection(db)

    print("Installing/loading Neo4j plugin")
    conn.execute("INSTALL neo4j;")
    conn.execute("LOAD neo4j;")

    print("Importing data from Neo4j")
    conn.execute('''
        CALL NEO4J_MIGRATE(
            'http://localhost:7474',
            'neo4j',
            '12345678',
            ['Application', 'Offer', 'Workflow', 'Event', 'Activity','CASE_AO','CASE_AW','CASE_WO','Resource'],
            ['CORR', 'DF_APPLICATION', 'DF_WORKFLOW', 'DF_OFFER', 'FROM','TO','FOR','OBSERVED']
        );
        ''')
    
    print("Running query")
    response = conn.execute("MATCH (n) RETURN count(n)")
    print(response)

main()