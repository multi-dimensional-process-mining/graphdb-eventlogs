import os
from typing import Optional, List, Sequence, Dict, Set, Any, Tuple

import neo4j
import pandas as pd
from pandas import DataFrame

from neo4j import GraphDatabase

class EventKnowledgeGraph:
    def __init__(self, batch_size, path, option_df_entity_type_in_label, verbose):

        self.batch_size = batch_size

        self.driver = self.start_connection()
        # ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised
        self.path_to_neo4j_import_directory = path

        self.option_df_entity_type_in_label = option_df_entity_type_in_label
        self.option_event_type_in_label = False

        self.verbose = verbose

        # set_common_strings()

    """METHODS TO CREATE A CONNECTION TO THE DB AND RUN QUERIES"""

    @staticmethod
    def start_connection():
        # begin config
        # connection to Neo4J database
        driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))
        # Neo4j can import local files only from its own import directory,
        # see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/
        # Neo4j's default configuration enables import from local file directory
        #    if it is not enabled, change Neo4j'c configuration file: dbms.security.allow_csv_import_from_file_urls=true
        # Neo4j's default import directory is <NEO4J_HOME>/import,
        #    to use this script
        #    - EITHER change the variable path_to_neo4j_import_directory to
        #    <NEO4J_HOME>/import and move the input files to this directory
        #    - OR set the import directory in Neo4j's configuration file: dbms.directories.import=
        #    see https://neo4j.com/docs/cypher-manual/current/clauses/load-csv/#query-load-csv-introduction
        return driver

    def close_connection(self):
        self.driver.close()

    def exec_query(self, query: str, **kwargs) -> Optional[List[Dict[str, Any]]]:
        """
        Write a transaction of the query to  the server and return the result
        @param query: string, query to be executed
        @return: The result of the query or None
        """

        def run_query(tx: neo4j.Transaction, query: str, **kwargs) -> Optional[List[Dict[str, Any]]]:
            """
                Run the query and return the result of the query
                @param tx: transaction class on which we can perform queries to the databasee
                @param query: string
                @return: The result of the query or None if there is no result
            """
            # get the results after the query is executed
            result = tx.run(query, kwargs).data()

            if result is not None and result != []:  # return the values if result is not none or empty list
                return result
            else:
                return None

        if self.verbose:
            print(query)

        with self.driver.session() as session:
            result = session.write_transaction(run_query, query, **kwargs)
            return result

    """FUNCTIONS TO ENSURE SYNCHRONOUS NAMING"""

    def get_df_label(self, label: str):
        """
        Create the df label based on self.option_DF_entity_type_in_label
        If not in entity type, add it as property to the label
        @param label: str, label that should be created in the DF
        @return:
        """
        if self.option_df_entity_type_in_label:
            return f'DF_{label.upper()}'
        else:
            return f'DF {{EntityType: "{label}"}} '

    def get_dfc_label(self, label: str):
        """
        Create the df label based on self.option_DF_entity_type_in_label
        If not in entity type, add it as property to the label
        @param label: str, label that should be created in the DF
        @return:
        """
        if self.option_df_entity_type_in_label:
            return f'DF_C_{label.upper()}'
        else:
            return f'DF_C {{EntityType: "{label}"}} '

    def get_event_label(self, label: str, properties: Optional[Dict[str, Any]] = None):
        """
        Create the df label based on self.option_DF_entity_type_in_label
        If not in entity type, add it as property to the label
        @param label: str, label that should be created in the DF
        @param properties:
        @return:
        """
        if properties is None:
            if self.option_event_type_in_label:
                return label
            else:
                return f'Event {{EventType: "{label}"}} '
        else:
            conditions = []
            for key, [value, is_string] in properties.items():
                if is_string:
                    conditions.append(f'{key}: "{value}"')
                else:
                    conditions.append(f'{key}: {value}')

            conditions = ", ".join(conditions)

            if self.option_event_type_in_label:
                return f'{label} {{{conditions}}}'
            else:
                return f'Event {{EventType: "{label}", {conditions}}} '

    """Define all queries and return their results (if required)"""

    def get_all_rel_types(self) -> List[str]:
        """
        Find all possible rel types
        @return:
        """
        # find all relations and return the distinct types
        q_request_rel_types = '''
            MATCH () - [rel] - () return DISTINCT type(rel) as rel_type
            '''
        # execute the query and store the result
        result = self.exec_query(q_request_rel_types)
        # in case there are no rel types, the result is None
        # return in this case an emtpy list
        if result is None:
            return []
        # store the results in a list
        result = [record["rel_type"] for record in result]
        return result

    def get_all_node_labels(self) -> Set[str]:
        """
        Find all possible node labels
        @return: Set of strings
        """

        # find all nodes and return the distinct labels
        q_request_node_labels = '''
            MATCH (n) return DISTINCT labels(n) as label
            '''

        # execute the query and store the result
        result = self.exec_query(q_request_node_labels)
        # in case there are no labels, return an empty set
        if result is None:
            return set([])
        # some nodes have multiple labels, which are returned as a list of labels
        # therefore we need to flatten the result and take the set
        result = set([record for sublist in result for record in sublist["label"]])
        return result

    def clear_db(self) -> None:
        # request all relation types, then we can delete them one by one
        relation_types = self.get_all_rel_types()
        # run one delete transaction per relationship type: smaller transactions require less memory and execute faster
        for rel_type in relation_types:
            q_delete_relation = f'''MATCH () -[r:{rel_type}]- () DELETE r'''
            self.exec_query(q_delete_relation)

        # delete all remaining relationships
        q_delete_all_relations = "MATCH () -[r]- () DELETE r"
        self.exec_query(q_delete_all_relations)

        # request all node labels, then we can delete them one by one
        node_types = self.get_all_node_labels()

        # run one delete transaction per node type type: smaller transactions require less memory and execute faster
        for node_type in node_types:
            q_delete_nodes = f'''MATCH (n:{node_type}) DELETE n'''
            self.exec_query(q_delete_nodes)

        # delete all remaining nodes
        q_delete_all_nodes = "MATCH (n) DELETE n"
        self.exec_query(q_delete_all_nodes)

    def set_constraints(self):
        query_constraint_unique_event_id = f'''
            CREATE CONSTRAINT unique_event_ids IF NOT EXISTS 
            FOR (e:Event) REQUIRE e.ID IS UNIQUE'''  # for implementation only (not required by schema or patterns)
        query_constraint_unique_entity_uid = f'''
                    CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                    FOR (en:Entity) REQUIRE en.uID IS UNIQUE'''   # required by core pattern
        query_constraint_unique_log_id = f'''
                    CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                    FOR (l:Log) REQUIRE l.ID IS UNIQUE'''   # required by core pattern

        self.exec_query(query_constraint_unique_event_id)
        self.exec_query(query_constraint_unique_entity_uid)
        self.exec_query(query_constraint_unique_log_id)

    """IMPORT EVENTS"""

    def create_events(self, input_path: str, labels: Optional[List[str]] = None) -> None:
        # Cypher does not recognize pd date times, therefore we convert the date times to the correct string format
        df_log: DataFrame = pd.read_csv(os.path.realpath(input_path), keep_default_na=True)
        df_log = df_log.rename(columns={"timestamp": "str_timestamp"})
        # df_log: DataFrame = EventKnowledgeGraph.change_timestamp_to_string(df_log)
        # Replace all missing values with "None"  as string
        df_log = df_log.fillna(value="None")

        # create a list of labels, "Event" is always a label of event nodes
        labels = ["Event"] + labels if labels is not None else ["Event"]
        labels = list(set(labels))

        # start with batch 0 and increment until everything is imported
        batch = 0
        while batch * self.batch_size < len(df_log):
            # import the events in batches, use the records of the log
            self.create_events_batch(
                batch=df_log[batch * self.batch_size:(batch + 1) * self.batch_size].to_dict('records'),
                labels=labels)
            batch += 1

        # once all events are imported, we convert the string timestamp to the timestamp as used in Cypher
        self.make_timestamp_date()

    def create_events_batch(self, batch: List[Dict[str, str]], labels: List[str]):
        """
        Create event nodes for each row in the batch with labels
        The properties of each row are also the property of the node
        @param batch: List[Dictionary[key: value]], the key and its values form properties of the event nodes
        @param labels: The labels of the event nodes
        @return: None,
        """

        # $batch is a variable we can add in tx.run, this allows us to use string properties
        # (keys in our dictionary are string)
        # return is required when using call and yield
        q_create_events_batch = f'''
            UNWIND $batch AS row
            CALL apoc.create.node({labels}, row) YIELD node
            RETURN count(*)
        '''

        self.exec_query(q_create_events_batch, batch=batch)

    def make_timestamp_date(self):
        """
        Convert the strings of the timestamp to the datetime as used in cypher
        Remove the str_timestamp property
        @return: None
        """
        q_make_timestamp = f'''
            MATCH (e:Event) WHERE e.timestamp IS NULL
            SET e.timestamp = DATETIME(e.str_timestamp)
            REMOVE e.str_timestamp
        '''
        self.exec_query(q_make_timestamp)

    @staticmethod
    def change_timestamp_to_string(df_log: DataFrame) -> DataFrame:
        """
        @param df_log: Dataframe, log of which the timestamps need to be converted to string
        @return: DataFrame with a str_timestamp instead of pandas datetime timestamp
        """
        df_log_timestamp_as_string = df_log.copy()
        # reformat the timestamp to a string which can be read as input for the Cypher graph
        # string is such that it can be converted to Timestamp within the graph environment
        df_log_timestamp_as_string["str_timestamp"] = df_log_timestamp_as_string["timestamp"].dt.strftime(
            '%Y-%m-%dT%H:%M:%S.%f')
        # remove the pd.datetime column
        df_log_timestamp_as_string.drop(["timestamp"], axis=1, inplace=True)
        return df_log_timestamp_as_string

    def filter_events_by_property(self, prop: str, values: Optional[List[str]] = None) -> None:
        if values is None:  # match all events that have a specific property
            # query to delete all events and its relationship with property
            q_filter_events = f"MATCH (e:Event) WHERE e.{prop} IS NOT NULL DETACH DELETE e"
        else:  # match all events with specific property and value
            # match all e and delete them and its relationship
            q_filter_events = f"MATCH (e:Event) WHERE e.{prop} in {values} DETACH DELETE e"

        # execute query
        self.exec_query(q_filter_events)

    """CREATE LOG AND CONNECT TO EVENTS"""

    def create_log(self):
        # create :log node with log_id as id and sublog id as sublogid
        q_create_log = f'''
            MATCH (e:Event) WHERE e.Log IS NOT NULL AND e.Log <> "nan"
            WITH e.Log AS log
            MERGE (:Log {{ID:log}})
        '''

        q_link_events_to_log = f'''
            MATCH (l:Log) 
            MATCH (e:Event {{Log: l.ID}}) 
            CREATE (l)-[:HAS]->(e)'''

        self.exec_query(q_create_log)

        self.exec_query(q_link_events_to_log)

    """CREATE ENTITIES"""

    def create_entity(self, property_name_id: str, entity_label: str, additional_label: Optional[str] = None,
                      properties: Optional[Dict[str, Any]] = None) -> None:
        # find events that contain the entity as property and not nan
        # save the value of the entity property as id and also whether it is a virtual entity
        # create a new entity node if it not exists yet with properties
        additional_label = ':' + additional_label if additional_label is not None else ""
        q_create_entity = f'''
                    MATCH (e:Event) WHERE {EventKnowledgeGraph.create_condition("e" , properties)}
                    WITH e.{property_name_id} AS id
                    MERGE (en:Entity:{entity_label}{additional_label} 
                            {{ID:id, uID:("{entity_label}_"+toString(id)), 
                            EntityType:"{entity_label}"}})
                    '''

        self.exec_query(q_create_entity)

    def correlate_events_to_entity(self, property_name_id: str, entity_label: str, properties: Optional[Dict[str, Any]] = None) -> None:
        # correlate events that contain a reference from an entity to that entity node

        q_correlate = f'''
            MATCH (e:Event) WHERE {EventKnowledgeGraph.create_condition("e" , properties)}
            MATCH (n:{entity_label}) WHERE e.{property_name_id} = n.ID 
            MERGE (e)-[:CORR]->(n)'''
        self.exec_query(q_correlate)

    def create_entity_relationships(self, relation_type: str, entity_name1: str, entity_name2: str,
                                    reference_from1to2: str) -> None:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        q_create_relation = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:{entity_name1} )
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:{entity_name2} )
                WHERE n1 <> n2 AND e2.{reference_from1to2} = n1.ID
            WITH DISTINCT n1,n2
            CREATE ( n1 ) <-[:{relation_type.upper()} ]- ( n2 )'''
        self.exec_query(q_create_relation)

    def reify_entity_relations(self, entity_name1: str, entity_name2: str, derived_entity: str,
                               ) -> None:
        # create from a relation edge a new :relation node
        # add a :REIFIED edge between the entities constituting this relationship and the new node
        q_reify_relation = f'''
                    MATCH ( n1 :{entity_name1} ) <- [rel:{derived_entity.upper()}]- ( n2:{entity_name2} )
                    CREATE (n1) <-[:REIFIED ]- (new:Entity:{derived_entity} {{ 
                        {entity_name1}: n1.ID,
                        {entity_name2}: n2.ID,
                        ID:toString(n1.ID)+"_"+toString(n2.ID),
                        EntityType: "{derived_entity}",
                        uID:"{derived_entity}_"+toString(n1.ID)+"_"+toString(n2.ID)}})
                        -[:REIFIED ]-> (n2)'''
        self.exec_query(q_reify_relation)

    def create_static_nodes_and_relations(self):
        # TODO no implementation yet (see if needed)
        pass

    @staticmethod
    def create_condition(name: str, properties: Dict[str, List[str]]) -> str:
        """
        Converts a dictionary into a string that can be used in a WHERE statement to find the correct node/relation
        @param name: str, indicating the name of the node/rel
        @param properties: Dictionary containing of property name and value
        @return: String that can be used in a where statement
        """

        condition_list = []
        for (key, conditions) in properties.items():
            for condition in conditions:
                condition_list.append(f"{name}.{key} {condition}")
        condition = " AND ".join(condition_list)
        return condition