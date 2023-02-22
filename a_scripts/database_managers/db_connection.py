from typing import Optional, List, Dict, Any

import neo4j
from neo4j import GraphDatabase


class DatabaseConnection(object):

    def __init__(self, uri: str, db_name: str, user: str, password: str, verbose: bool = False):
        self.db_name = db_name
        self.driver = self.start_connection(uri, user, password)
        self.verbose = verbose

    @staticmethod
    def start_connection(uri: str, user: str, password: str):
        # begin config
        # connection to Neo4J database
        driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_lifetime=200)
        return driver

    def close_connection(self):
        self.driver.close()

    def exec_query(self, function, **kwargs):
        result = function(**kwargs)
        query = result.query_string
        kwargs = result.kwargs

        return self._exec_query(query, **kwargs)

    def _exec_query(self, query: str, database: str = None, **kwargs) -> Optional[List[Dict[str, Any]]]:
        """
        Write a transaction of the query to  the server and return the result
        @param query: string, query to be executed
        @param database: string, Name of the database
        @return: The result of the query or None
        """

        def run_query(tx: neo4j.Transaction, _query: str, **_kwargs) -> Optional[List[Dict[str, Any]]]:
            """
                Run the query and return the result of the query
                @param tx: transaction class on which we can perform queries to the database
                @param _query: string
                @return: The result of the query or None if there is no result
            """
            # get the results after the query is executed
            try:
                _result = tx.run(_query, _kwargs).data()
            except Exception as inst:
                self.close_connection()
                print(inst)
            else:
                if _result is not None and _result != []:  # return the values if result is not none or empty list
                    return _result
                else:
                    return None

        if self.verbose:
            print(query)

        if database is None:
            database = self.db_name

        with self.driver.session(database=database) as session:
            result = session.write_transaction(run_query, query, **kwargs)
            return result
