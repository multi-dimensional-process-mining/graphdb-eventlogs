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
    def __init__(self, uri: str, db_name: str, user: str, password: str, batch_size: int,
                 option_df_entity_type_in_label: bool,
                 verbose: bool):

        self.batch_size = batch_size
        self.db_name = db_name

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
        driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_lifetime=200)
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

    def exec_query(self, query: str, database: str = None, **kwargs) -> Optional[List[Dict[str, Any]]]:
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

        if database is None:
            database = self.db_name

        with self.driver.session(database=database) as session:
            result = session.write_transaction(run_query, query, **kwargs)
            return result

    # endregion

    # region DATABASE MAINTENANCE

    def clear_db(self, db_name) -> None:

        delete_query = f'''
            CALL apoc.periodic.iterate(
                'MATCH (n) RETURN n',
                 'DETACH DELETE n',
                  {{batchSize:1000, parallel:false}})
        '''

        q_replace_database = f'''
                    CREATE OR REPLACE DATABASE {self.db_name}
                    WAIT
                '''

        # self.exec_query(q_switch_to_system)
        self.exec_query(q_replace_database, database="system")
        # self.exec_query(q_switch_to_db)

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

    def create_events(self, input_path: str, file_name: str, na_values: str = None, dtype_dict: Dict[str, str] = None,
                      labels: Optional[List[str]] = None, mapping: Dict[str, str] = None,
                      datetime_formats: Dict[str, str] = None) -> None:
        # Cypher does not recognize pd date times, therefore we convert the date times to the correct string format
        if dtype_dict is None:
            df_log: DataFrame = pd.read_csv(os.path.realpath(input_path + file_name), keep_default_na=True)
        else:
            df_log: DataFrame = pd.read_csv(os.path.realpath(input_path + file_name), keep_default_na=True,
                                            dtype=dtype_dict)

        if mapping is not None:
            df_log = df_log.rename(columns=mapping)

        # rename coplumns that need to be converted to avoid duplicate conversion (will return in an error)
        if datetime_formats is not None:
            mapping_timestamps = {key: key + "NotConverted" for key in datetime_formats.keys()}
            df_log = df_log.rename(columns=mapping_timestamps)
        # df_log: DataFrame = EventKnowledgeGraph.change_timestamp_to_string(df_log)
        # Replace all missing values with "None"  as string
        # na_values = "None" if na_values is None else na_values
        df_log = df_log.fillna(value="None")

        # create a list of labels, "Event" is always a label of event nodes
        labels = ["Event"] + labels if labels is not None else ["Event"]
        labels = list(set(labels))

        # start with batch 0 and increment until everything is imported
        batch = 0
        print("\n")
        pbar = tqdm(total=math.ceil(len(df_log) / self.batch_size), position=0)
        while batch * self.batch_size < len(df_log):
            pbar.set_description(f"Loading events from {file_name} from batch {batch}")

            # import the events in batches, use the records of the log
            batch_without_nans = [{k: v for k, v in m.items() if v == v and v is not None} for m in
                                  df_log[batch * self.batch_size:(batch + 1) * self.batch_size].to_dict(
                                      orient='records')]
            self.create_events_batch(
                batch=batch_without_nans,
                na_values=na_values,
                labels=labels)
            pbar.update(1)
            batch += 1
        pbar.close()
        # once all events are imported, we convert the string timestamp to the timestamp as used in Cypher
        if datetime_formats is not None:
            for attribute, datetime_format in datetime_formats.items():
                self.make_timestamp_date(attribute, datetime_format)

    def create_events_batch(self, batch: List[Dict[str, str]], na_values: str, labels: List[str]):
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
        # batch_no_nans = []
        # for row in batch:
        #     row_no_nans = {k: v if not math.isnan(v) else na_values for k, v in row.items() }
        # batch = [{k: v if not math.isnan(v) else na_values for k, v in row.items()} for row in batch]

        q_create_events_batch = f'''
            UNWIND $batch AS row
            CALL apoc.create.node({labels}, row) YIELD node
            RETURN count(*)
        '''

        self.exec_query(q_create_events_batch, batch=batch)

    def make_timestamp_date(self, attribute, datetimeObject):
        """
        Convert the strings of the timestamp to the datetime as used in cypher
        Remove the str_timestamp property
        @return: None
        """
        q_make_timestamp = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.{attribute}NotConverted IS NOT NULL 
            WITH e, e.{attribute}NotConverted+'{datetimeObject.offset}' as timezoned_dt
            WITH e, datetime(apoc.date.convertFormat(timezoned_dt, '{datetimeObject.format}', '{datetimeObject.convert_to}')) as converted
            RETURN e, converted",
            "SET e.{attribute} = converted
            REMOVE e.{attribute}NotConverted",
            {{batchSize:10000, parallel:false}})
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
            MATCH (e:Event) WHERE e.log IS NOT NULL AND e.log <> "nan"
            WITH e.log AS log
            MERGE (:Log {{ID:log}})
        '''

        q_link_events_to_log = f'''
            CALL apoc.periodic.iterate(
                'MATCH (l:Log) 
                MATCH (e:Event {{Log: l.ID}})
                RETURN e, l', 
                'MERGE (l)-[:HAS]->(e)',
                {{batchSize:{self.batch_size}}})
            '''

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
        
            CALL apoc.periodic.iterate(
                'MATCH (e:Event) WHERE {EventKnowledgeGraph.create_condition("e", properties)}
                MATCH (n:{entity_label}) WHERE e.{property_name_id} = n.ID
                RETURN e, n',
                'MERGE (e)-[:CORR]->(n)',
                {{batchSize: {self.batch_size}}})
                '''
        self.exec_query(q_correlate)

    def correlate_events_to_derived_entity(self, derived_entity: str) -> None:
        # correlate events that are related to an entity which is reified into a new entity to the new reified entity
        q_correlate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{EntityType:"{derived_entity}"}} )
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{EntityType:"{derived_entity}"}} )
            MERGE (e)-[:CORR]->(r)'''
        self.exec_query(q_correlate)

    def create_entity_relationships(self, relation_type: str, entity_label_from_node: str, entity_label_to_node: str,
                                    reference_in_event_to_to_node: str) -> None:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        q_create_relation = f'''
            MATCH ( e1 : Event ) -[:CORR]-> ( n1:{entity_label_to_node} )
            MATCH ( e2 : Event ) -[:CORR]-> ( n2:{entity_label_from_node} )
                WHERE n1 <> n2 AND e2.{reference_in_event_to_to_node} = n1.ID
            WITH DISTINCT n1,n2
            MERGE ( n1 ) <-[:{relation_type.upper()} {{Type:"Rel"}}]- ( n2 )'''
        self.exec_query(q_create_relation)

    def reify_entity_relations(self, entity_name1: str, entity_name2: str, derived_entity: str,
                               ) -> None:
        # create from a relation edge a new :relation node
        # add a :REIFIED edge between the entities constituting this relationship and the new node
        q_reify_relation = f'''
                    MATCH ( n1 :{entity_name1} ) <- [rel:{derived_entity.upper()}]- ( n2:{entity_name2} )
                    MERGE (n1) <-[:REIFIED ]- (new:Entity:{derived_entity} {{ 
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
         CALL apoc.periodic.iterate(
            'MATCH ( n : {entity_name} ) <-[:CORR]- (e)
            WITH n , e as nodes ORDER BY e.timestamp,e.order_nr, ID(e)
            WITH n , collect ( nodes ) as nodeList
            UNWIND range(0,size(nodeList)-2) AS i
            WITH n , nodeList[i] as first, nodeList[i+1] as second
            RETURN first, second',
            'MERGE ( first ) -[:{df_entity_string} {{EntityType: "{entity_name}", Type:"DF"}}]->( second )',
            {{batchSize: {self.batch_size}}})
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
                    MERGE (n1)-[:{df_entity_string} {{EntityType: "{entity_name}", Count:size(rels), Type:"DF"}}]->(n2)
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

    def aggregate_df_relations(self, entity_type: Optional[str] = None, classifiers: Optional[List[str]] = None,
                               df_threshold: int = 0, relative_df_threshold: float = 0) -> None:
        # add relations between classes when desired
        if entity_type is None and classifiers is None:
            q_create_dfc = f'''
                   MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
                   MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                   WHERE c1.Type = c2.Type AND n.EntityType = df.EntityType
                   WITH n.EntityType as EType,c1 AS c1,count(df) AS df_freq,c2 AS c2
                   MERGE ( c1 ) -[rel2:DF_C {{EntityType:EType, Type:"DF_C"}}]-> ( c2 ) ON CREATE SET rel2.count=df_freq
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
            classifier_string = "_".join(classifiers)
            df_label = self.get_df_label(entity_type)
            dfc_label = self.get_dfc_label(entity_type)
            q_create_dfc = f'''
                    MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:{df_label} {{EntityType: '{entity_type}'}}]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
                    MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                    WHERE n.EntityType = df.EntityType AND c1.Type = "{classifier_string}" AND c2.Type="{classifier_string}"
                    WITH n.EntityType as EType,c1,count(df) AS df_freq,c2
                    MERGE ( c1 ) -[rel2:{dfc_label} {{EntityType: '{entity_type}', Type:"DF_C"}}]-> ( c2 ) 
                    ON CREATE SET rel2.count=df_freq'''
            self.exec_query(q_create_dfc)
        else:
            # aggregate only for a specific entity type and event classifier
            # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
            classifier_string = "_".join(classifiers)
            df_label = self.get_df_label(entity_type)
            dfc_label = self.get_dfc_label(entity_type)
            q_create_dfc = f'''
                    MATCH ( c1 : Class ) <-[:OBSERVED]- ( e1 : Event ) -[df:{df_label} {{EntityType: '{entity_type}'}}]-> ( e2 : Event ) -[:OBSERVED]-> ( c2 : Class )
                    MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                    WHERE n.EntityType = df.EntityType AND c1.Type = "{classifier_string}" AND c2.Type="{classifier_string}"
                    WITH n.EntityType as EntityType,c1,count(df) AS df_freq,c2
                    WHERE df_freq > {df_threshold}
                    OPTIONAL MATCH ( c2 : Class ) <-[:OBSERVED]- ( e2b : Event ) -[df2:DF]-> ( e1b : Event ) -[:OBSERVED]-> ( c1 : Class )
                    WITH EntityType as EType,c1,df_freq,count(df2) AS df_freq2,c2
                    WHERE (df_freq*{relative_df_threshold} > df_freq2)
                    MERGE ( c1 ) -[rel2:{dfc_label} {{EntityType: '{entity_type}', Type:"DF_C"}}]-> ( c2 ) 
                    ON CREATE SET rel2.count=df_freq'''
            self.exec_query(q_create_dfc)

    # endregion

    # region CREATE CLASSES
    def create_class(self, label: str = "Event", required_keys: Optional[Sequence[str]] = None,
                     ids: Optional[Sequence[str]] = None) -> None:
        # add values if those are None
        required_keys = required_keys if required_keys else ["activity", "lifecycle"]
        ids = ids if ids else ["c_id", "name", "lifecycle"]

        # make sure first element of id list is cID
        if "cID" not in ids:
            ids = ["cID"] + ids

        # reformat to e.key with alias to create with condition
        alias_keys = [f"e.{key} AS {key}" for key in required_keys]
        with_condition = ' , '.join([f"{key}" for key in alias_keys])

        # reformat to where e.key is not null to create with condition
        not_null_keys = [f"e.{key} IS NOT NULL" for key in required_keys]
        where_condition_not_null = " AND ".join([f"{key}" for key in not_null_keys])

        # create a combined id in string format
        ID = "+".join([f"{key}" for key in required_keys])
        class_label = "_".join([f"{key}" for key in required_keys])
        # add to the keys
        required_keys = [ID] + required_keys

        node_properties = ', '.join([f"{_id}: {key}" for _id, key in zip(ids, required_keys)])
        node_properties += f", Type: '{ID}'"  # save ID also as string that captures the type

        # create new class nodes for event nodes that match the condition
        q_create_ec = f'''
                MATCH ( e : {label} )
                WHERE {where_condition_not_null}
                WITH distinct {with_condition}
                MERGE ( c : Class : Class_{class_label} {{ {node_properties} }})'''
        self.exec_query(q_create_ec)

        # reformat to e.key
        required_keys = [f"e.{key}" for key in required_keys]
        where_link_condition = ' AND '.join([f"c.{_id} = {key}" for _id, key in zip(ids[1:], required_keys[1:])])

        # Create :OBSERVED relation between the class and events
        q_link_event_to_class = f'''
            CALL apoc.periodic.iterate(
                'MATCH ( c : Class_{class_label})
                MATCH ( e : Event ) WHERE {where_link_condition}
                RETURN e, c',
                'MERGE ( e ) -[:OBSERVED]-> ( c )',
                {{batchSize: {self.batch_size}}})                
            '''
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
