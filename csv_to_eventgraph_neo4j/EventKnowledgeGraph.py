import csv
import math
import os
from tqdm import tqdm
from typing import Optional, List, Sequence, Dict, Set, Any, Tuple

import neo4j
import pandas as pd
from pandas import DataFrame

from neo4j import GraphDatabase


class EventKnowledgeGraph:
    def __init__(self, uri: str, user: str, password: str, batch_size: int,
                 option_df_entity_type_in_label: bool,
                 verbose: bool):

        self.batch_size = batch_size

        self.driver = self.start_connection(uri, user, password)
        # ensure to allocate enough memory to your database: dbms.memory.heap.max_size=20G advised

        self.option_df_entity_type_in_label = option_df_entity_type_in_label
        self.option_event_type_in_label = False

        self.verbose = verbose

        # set_common_strings()

    # region CREATE CONNECTION TO DATABASE AND RUN QUERIES

    @staticmethod
    def start_connection(uri: str, user: str, password: str):
        # begin config
        # connection to Neo4J database
        driver = GraphDatabase.driver(uri, auth=(user, password))
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

    # endregion

    # region DATABASE MAINTENANCE

    def clear_db(self) -> None:

        delete_query = f'''
            CALL apoc.periodic.iterate(
                'MATCH (n) RETURN n',
                 'DETACH DELETE n',
                  {{batchSize:1000, parallel:true}})
        '''
        self.exec_query(delete_query)

    def set_constraints(self):
        query_constraint_unique_event_id = f'''
            CREATE CONSTRAINT unique_event_ids IF NOT EXISTS 
            FOR (e:Event) REQUIRE e.ID IS UNIQUE'''  # for implementation only (not required by schema or patterns)
        query_constraint_unique_entity_uid = f'''
                    CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                    FOR (en:Entity) REQUIRE en.uID IS UNIQUE'''  # required by core pattern
        query_constraint_unique_log_id = f'''
                    CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                    FOR (l:Log) REQUIRE l.ID IS UNIQUE'''  # required by core pattern

        self.exec_query(query_constraint_unique_event_id)
        self.exec_query(query_constraint_unique_entity_uid)
        self.exec_query(query_constraint_unique_log_id)

    # endregion

    # region CONSISTENT NAMING FOR LABELS

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
            return f'DF'

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
            return f'DF_C'

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

    # endregion

    # region FUNCTIONS TO RETRIEVE ALL REL TYPES AND NODE LABELS
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

    # endregion

    # region IMPORT EVENTS

    @staticmethod
    def get_headers(local_file):
        dataset_list = []
        header_csv = []
        i = 0
        with open(local_file) as f:
            reader = csv.reader(f)
            for row in reader:
                if i == 0:
                    header_csv = list(row)
                    i += 1
                else:
                    dataset_list.append(row)

        return header_csv

    def create_events(self, input_path: str, labels: Optional[List[str]] = None) -> None:

        # header = EventKnowledgeGraph.get_headers(input_path + file_name)
        #
        # map = [f'{attribute}: line.{attribute}' for attribute in header]
        # map_string = ", ".join(map)
        #
        # query = f'''
        #     :auto LOAD CSV WITH HEADERS from "file:///{file_name}\" as line
        #     CALL {{
        #         WITH line
        #         CREATE (e:Event {{{map_string}}})
        #     }} IN TRANSACTIONS OF 500 ROWS
        # '''
        #
        # self.exec_query(query)

        # query = f'USING PERIODIC COMMIT LOAD CSV WITH HEADERS FROM \"file:///{file_name}\" as line'
        # for col in log_header:
        #     if col == 'idx':
        #         column = f'toInt(line.{col})'
        #     elif col in ['timestamp', 'start', 'end']:
        #         column = f'datetime(line.{col})'
        #     else:
        #         column = 'line.' + col
        #
        #     if log_header.index(col) == 0 and log_id != "":
        #         new_line = f' CREATE (e:Event {{Log: "{log_id}",{col}: {column},'
        #     elif log_header.index(col) == 0:
        #         new_line = f' CREATE (e:Event {{ {col}: {column},'
        #     else:
        #         new_line = f' {col}: {column},'
        #     if log_header.index(col) == len(log_header) - 1:
        #         new_line = f' {col}: {column} }})'
        #
        #     query = query + new_line
        # return query
        #
        #
        #
        #
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
        print("\n")
        pbar = tqdm(total=math.ceil(len(df_log) / self.batch_size), position=0)
        while batch * self.batch_size < len(df_log):
            pbar.set_description(f"Loading events from batch {batch}")
            # import the events in batches, use the records of the log
            self.create_events_batch(
                batch=df_log[batch * self.batch_size:(batch + 1) * self.batch_size].to_dict('records'),
                labels=labels)
            pbar.update(1)
            batch += 1
        pbar.close()
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
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.timestamp IS NULL RETURN e",
            "SET e.timestamp = DATETIME(e.str_timestamp)
            REMOVE e.str_timestamp",
            {{batchSize:10000, parallel:true}})
        '''
        self.exec_query(q_make_timestamp)

    def filter_events_by_property(self, prop: str, values: Optional[List[str]] = None) -> None:
        if values is None:  # match all events that have a specific property
            # query to delete all events and its relationship with property
            q_filter_events = f"MATCH (e:Event) WHERE e.{prop} IS NOT NULL DETACH DELETE e"
        else:  # match all events with specific property and value
            # match all e and delete them and its relationship
            q_filter_events = f"MATCH (e:Event) WHERE e.{prop} in {values} DETACH DELETE e"

        # execute query
        self.exec_query(q_filter_events)

    # endregion

    # region CREATE LOG AND CONNECT TO EVENTS

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

    # endregion

    # region CREATE ENTITIES

    def create_entity(self, property_name_id: str, entity_label: str, additional_label: Optional[str] = None,
                      properties: Optional[Dict[str, Any]] = None) -> None:
        # find events that contain the entity as property and not nan
        # save the value of the entity property as id and also whether it is a virtual entity
        # create a new entity node if it not exists yet with properties
        additional_label = ':' + additional_label if additional_label is not None else ""
        q_create_entity = f'''
                    MATCH (e:Event) WHERE {EventKnowledgeGraph.create_condition("e", properties)}
                    WITH e.{property_name_id} AS id
                    MERGE (en:Entity:{entity_label}{additional_label} 
                            {{ID:id, uID:("{entity_label}_"+toString(id)), 
                            EntityType:"{entity_label}"}})
                    '''

        self.exec_query(q_create_entity)

    def correlate_events_to_entity(self, property_name_id: str, entity_label: str,
                                   properties: Optional[Dict[str, Any]] = None) -> None:
        # correlate events that contain a reference from an entity to that entity node

        q_correlate = f'''
            MATCH (e:Event) WHERE {EventKnowledgeGraph.create_condition("e", properties)}
            MATCH (n:{entity_label}) WHERE e.{property_name_id} = n.ID 
            MERGE (e)-[:CORR]->(n)'''
        self.exec_query(q_correlate)

    def correlate_events_to_derived_entity(self, derived_entity: str) -> None:
        # correlate events that are related to an entity which is reified into a new entity to the new reified entity
        q_correlate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{EntityType:"{derived_entity}"}} )
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{EntityType:"{derived_entity}"}} )
            MERGE (e)-[:CORR]->(r)'''
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

    # endregion

    # region CREATE DIRECTLY FOLLOWS RELATIONS

    def create_directly_follows(self, entity_name: str) -> None:
        # find the specific entities and events with a certain label correlated to that entity
        # order all events by time, order_nr and id grouped by a node n
        # collect the sorted nodes as a list
        # unwind the list from 0 to the one-to-last node
        # find neighbouring nodes and add a edge between
        df_entity_string = self.get_df_label(entity_name)

        q_create_df = f'''
            MATCH ( n : {entity_name} ) <-[:CORR]- (e)
            WITH n , e as nodes ORDER BY e.timestamp,e.order_nr, ID(e)
            WITH n , collect ( nodes ) as nodeList
            UNWIND range(0,size(nodeList)-2) AS i
            WITH n , nodeList[i] as first, nodeList[i+1] as second
            CREATE ( first ) -[:{df_entity_string} {{EntityType: "{entity_name}"}}]->( second )
        '''

        self.exec_query(q_create_df)

    def merge_duplicate_df(self, entity_name: str) -> None:

        df_entity_string = self.get_df_label(entity_name)

        q_merge_duplicate_rel = f'''
                    MATCH (n1:Event)-[r:{df_entity_string} {{EntityType: "{entity_name}"}}]->(n2:Event)
                    WITH n1, n2, collect(r) AS rels
                    WHERE size(rels) > 1
                    UNWIND rels AS r // only include this and the next line if you want to remove the existing relationships
                    DELETE r
                    MERGE (n1)-[:{df_entity_string} {{EntityType: "{entity_name}", count:size(rels)}}]->(n2)
                '''
        self.exec_query(q_merge_duplicate_rel)

    def delete_parallel_directly_follows_derived(self, derived_entity_type, original_entity_type):
        df_derived_entity = self.get_df_label(derived_entity_type)
        df_original_entity = self.get_df_label(original_entity_type)

        q_delete_df = f'''
            MATCH (e1:Event) -[df:{df_derived_entity} {{EntityType: "{derived_entity_type}"}}]-> (e2:Event)
            WHERE (e1:Event) -[:{df_original_entity} {{EntityType: "{original_entity_type}"}}]-> (e2:Event)
            DELETE df'''

        self.exec_query(q_delete_df)

    def aggregate_df_relations(self, entity_type: Optional[str] = None, event_cl: Optional[str] = None,
                               df_threshold: int = 0, relative_df_threshold: float = 0) -> None:
        # add relations between classes when desired
        if entity_type is None and event_cl is None:
            q_create_dfc = f'''
                   MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
                   MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                   WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType
                   WITH n.EntityType as EType,c1 AS c1,count(df) AS df_freq,c2 AS c2
                   MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq
                   '''

            q_change_label = f'''
                MATCH ( c1 ) -[rel2:DF_C]-> ( c2 ) 
                WITH rel2, rel2.EntityType as EType, rel2.count AS df_freq, c1, c2
                   CALL apoc.do.when(
                    {self.option_df_entity_type_in_label},
                    "RETURN 'DF_C_'+EType as DFLabel",
                    "RETURN 'DF_C' as DFLabel",
                    {{EType:EType}})
                    YIELD value
                      
               CALL apoc.refactor.setType(rel2, value.DFLabel)
               YIELD input, output
               RETURN input, output
            '''
            self.exec_query(q_create_dfc)
            self.exec_query(q_change_label)

        elif df_threshold == 0 and relative_df_threshold == 0:  # corresponds to aggregateDFrelations &  aggregateDFrelationsForEntities in graphdb-eventlogs
            # aggregate only for a specific entity type and event classifier

            df_label = self.get_df_label(entity_type)
            dfc_label = self.get_dfc_label(entity_type)
            q_create_dfc = f'''
                    MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:{df_label} {{EntityType: '{entity_type}'}}]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
                    MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                    WHERE n.EntityType = df.EntityType AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
                    WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
                    MERGE ( c1 ) -[rel2:{dfc_label} {{EntityType: '{entity_type}'}}]-> ( c2 ) 
                    ON CREATE SET rel2.count=df_freq'''
            self.exec_query(q_create_dfc)
        else:
            # aggregate only for a specific entity type and event classifier
            # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
            df_label = self.get_df_label(entity_type)
            dfc_label = self.get_dfc_label(entity_type)
            q_create_dfc = f'''
                    MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:{df_label} {{EntityType: '{entity_type}'}}]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
                    MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                    WHERE n.EntityType = df.EntityType AND c1.Type = "{event_cl}" AND c2.Type="{event_cl}"
                    WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
                    WHERE df_freq > {df_threshold}
                    OPTIONAL MATCH ( c2 : Class ) <-[:OBSERVED]- ( e2b : Event ) -[df2:DF]-> ( e1b : Event ) -[:OBSERVED]-> ( c1 : Class )
                    WITH EntityType as EType,c1,df_freq,count(df2) AS df_freq2,c2
                    WHERE (df_freq*{relative_df_threshold} > df_freq2)
                    MERGE ( c1 ) -[rel2:{dfc_label} {{EntityType: '{entity_type}'}}]-> ( c2 ) 
                    ON CREATE SET rel2.count=df_freq'''
            self.exec_query(q_create_dfc)

    # endregion

    # region CREATE CLASSES
    def create_class(self, label: str = "Event", required_keys: Optional[Sequence[str]] = None,
                     ids: Optional[Sequence[str]] = None) -> None:
        # add values if those are None
        required_keys = required_keys if required_keys else ["Activity", "Lifecycle"]
        ids = ids if ids else ["cID", "Name", "Lifecycle"]

        # make sure first element of id list is cID
        if "cID" not in ids:
            ids = ["cID"] + ids

        # reformat to e.key with alias
        alias_keys = [f"e.{key} AS {key}" for key in required_keys]

        # create the where condition
        with_condition = ' , '.join([f"{key}" for key in alias_keys])
        # create a combined id in string format
        ID = "+".join([f"{key}" for key in required_keys])
        class_label = "_".join([f"{key}" for key in required_keys])
        # add to the keys
        required_keys = [ID] + required_keys

        node_properties = ', '.join([f"{_id}: {key}" for _id, key in zip(ids, required_keys)])
        node_properties += f", Type: '{ID}'"  # save ID also as string that captures the type

        # create new class nodes for event nodes that match the condition
        q_create_ec = f'''
                MATCH ( e : {label} ) WITH distinct {with_condition}
                MERGE ( c : Class : Class_{class_label} {{ {node_properties} }})'''
        self.exec_query(q_create_ec)

        # reformat to e.key
        required_keys = [f"e.{key}" for key in required_keys]
        where_link_condition = ' AND '.join([f"c.{_id} = {key}" for _id, key in zip(ids[1:], required_keys[1:])])

        # Create :OBSERVED relation between the class and events
        q_link_event_to_class = f'''
                MATCH ( c : Class_{class_label})
                MATCH ( e : Event ) WHERE {where_link_condition}
                CREATE ( e ) -[:OBSERVED]-> ( c )'''
        self.exec_query(q_link_event_to_class)

    # endregion

    # region CREATE STATIC NODES AND RELATIONS
    def create_static_nodes_and_relations(self):
        # TODO no implementation yet (see if needed)
        pass

    # endregion

    # region STATIC METHODS
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

    # endregion
