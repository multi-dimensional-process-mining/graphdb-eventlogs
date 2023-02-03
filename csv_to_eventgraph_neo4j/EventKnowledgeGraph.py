import math

from tqdm import tqdm
from typing import Optional, List, Dict, Set, Any

from csv_to_eventgraph_neo4j.event_table import EventTables
from csv_to_eventgraph_neo4j.performance_handling import Performance
from csv_to_eventgraph_neo4j.semantic_header import Entity
from csv_to_eventgraph_neo4j.semantic_header_lpg import SemanticHeaderLPG, EntityLPG, RelationLPG

import neo4j
import pandas as pd
from pandas import DataFrame

from neo4j import GraphDatabase

from tabulate import tabulate


class EventKnowledgeGraph:
    def __init__(self, uri: str, db_name: str, user: str, password: str, batch_size: int,
                 verbose: bool, event_tables: EventTables, use_sample: bool = False,
                 semantic_header: SemanticHeaderLPG = None,
                 perf: Performance = None):

        self.batch_size = batch_size
        self.db_name = db_name

        # ensure to allocate enough memory to your database: dbms.memory.heap.max_size=5G advised
        self.driver = self.start_connection(uri, user, password)

        self.option_event_type_in_label = False

        self.verbose = verbose
        self.use_sample = use_sample

        self.event_tables = event_tables
        self.semantic_header = semantic_header

        self.perf = perf

    # region CREATE CONNECTION TO DATABASE AND RUN QUERIES

    @staticmethod
    def start_connection(uri: str, user: str, password: str):
        # begin config
        # connection to Neo4J database
        driver = GraphDatabase.driver(uri, auth=(user, password), max_connection_lifetime=200)
        return driver

    def close_connection(self):
        self.driver.close()

    def exec_query(self, query: str, database: str = None, **kwargs) -> Optional[List[Dict[str, Any]]]:
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
            _result = tx.run(_query, _kwargs).data()

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

    # endregion

    # region DATABASE MAINTENANCE
    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    @staticmethod
    def get_dfc_label(entity_type: str, include_label_in_dfc: bool):
        if include_label_in_dfc:
            return f'DF_C_{entity_type.upper()}'
        else:
            return f'DF_C'

    def clear_db(self) -> None:

        # delete_query = f'''
        #     CALL apoc.periodic.iterate(
        #         'MATCH (n) RETURN n',
        #          'DETACH DELETE n',
        #           {{batchSize:1000, parallel:false}})
        # '''

        q_replace_database = f'''
                    CREATE OR REPLACE DATABASE {self.db_name}
                    WAIT
                '''

        self.exec_query(q_replace_database, database="system")
        self._write_message_to_performance("Cleared database")

    def set_constraints(self) -> None:
        query_constraint_unique_event_id = f'''
            CREATE CONSTRAINT unique_event_ids IF NOT EXISTS 
            FOR (e:Event) REQUIRE e.ID IS UNIQUE'''  # for implementation only (not required by schema or patterns)
        self.exec_query(query_constraint_unique_event_id)
        self._write_message_to_performance("Constraint on unique event IDs is set")

        query_constraint_unique_entity_uid = f'''
                    CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                    FOR (en:Entity) REQUIRE en.uID IS UNIQUE'''  # required by core pattern
        self.exec_query(query_constraint_unique_entity_uid)
        self._write_message_to_performance("Constraint on unique entity uIDs is set")

        query_constraint_unique_log_id = f'''
                    CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                    FOR (l:Log) REQUIRE l.ID IS UNIQUE'''  # required by core pattern
        self.exec_query(query_constraint_unique_log_id)
        self._write_message_to_performance("Constraint on unique log IDs is set")

    # endregion

    # region CONSISTENT NAMING FOR LABELS

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

    def create_events(self) -> None:
        # Cypher does not recognize pd date times, therefore we convert the date times to the correct string format
        for event_table in self.event_tables.structures:
            labels = event_table.labels
            file_directory = event_table.file_directory
            for file_name in event_table.file_names:
                # read and import the events
                df_log = event_table.prepare_data_set(file_directory, file_name, self.use_sample)
                df_log["justImported"] = True
                self._create_event_nodes_from_event_table(labels, df_log, file_name)
                self._write_message_to_performance(f"Imported events from event table {event_table.name}: {file_name}")

                # once all events are imported, we convert the string timestamp to the timestamp as used in Cypher
                datetime_formats = event_table.get_datetime_formats()
                for attribute, datetime_format in datetime_formats.items():
                    self._make_timestamp_date(attribute, datetime_format)

                self._write_message_to_performance(
                    f"Reformatted timestamps from events from event table {event_table.name}: {file_name}")

                for boolean in (True, False):
                    attribute_values_pairs_filtered = event_table.get_attribute_value_pairs_filtered(exclude=boolean)
                    for name, values in attribute_values_pairs_filtered:
                        self._filter_events_by_property(name, values, exclude=boolean)

                self._write_message_to_performance(
                    f"Filtered the events from event table {event_table.name}: {file_name}")

                # finalize the import
                self._finalize_import_events()

    def _create_event_nodes_from_event_table(self, labels, df_log, file_name):
        # start with batch 0 and increment until everything is imported
        batch = 0
        print("\n")
        pbar = tqdm(total=math.ceil(len(df_log) / self.batch_size), position=0)
        while batch * self.batch_size < len(df_log):
            pbar.set_description(f"Loading events from {file_name} from batch {batch}")

            # import the events in batches, use the records of the log
            batch_without_nans = [{k: v for k, v in m.items() if not pd.isna(v) and v is not None} for m in
                                  df_log[batch * self.batch_size:(batch + 1) * self.batch_size].to_dict(
                                      orient='records')]
            self._create_events_batch(
                batch=batch_without_nans,
                labels=labels)
            pbar.update(1)
            batch += 1
        pbar.close()

    def _create_events_batch(self, batch: List[Dict[str, str]], labels: List[str]):
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

    def _make_timestamp_date(self, attribute, datetime_object):
        """
        Convert the strings of the timestamp to the datetime as used in cypher
        Remove the str_timestamp property
        @return: None
        """
        q_make_timestamp = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.{attribute} IS NOT NULL AND e.justImported = True 
            WITH e, e.{attribute}NotConverted+'{datetime_object.timezone_offset}' as timezone_dt
            WITH e, datetime(apoc.date.convertFormat(timezone_dt, '{datetime_object.format}', 
                '{datetime_object.convert_to}')) as converted
            RETURN e, converted",
            "SET e.{attribute} = converted
            REMOVE e.{attribute}NotConverted",
            {{batchSize:10000, parallel:false}})
        '''
        self.exec_query(q_make_timestamp)

    def _finalize_import_events(self):
        q_set_just_imported_to_false = f'''
        CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.justImported = True 
            RETURN e",
            "REMOVE e.justImported",
            {{batchSize:10000, parallel:false}})
        '''

        self.exec_query(q_set_just_imported_to_false)

    def _filter_events_by_property(self, prop: str, values: Optional[List[str]] = None, exclude=True) -> None:
        if values is None:  # match all events that have a specific property
            negation = "NOT" if exclude else ""
            # query to delete all events and its relationship with property
            q_filter_events = f"MATCH (e:Event {{e.justImported: True}}) " \
                              f"WHERE e.{prop} IS {negation} NULL " \
                              f"DETACH DELETE e"
        else:  # match all events with specific property and value
            negation = "" if exclude else "NOT"
            # match all e and delete them and its relationship
            q_filter_events = f"MATCH (e:Event {{e.justImported: True}})" \
                              f"WHERE {negation} e.{prop} in {values}" \
                              f"DETACH DELETE e"

        # execute query
        self.exec_query(q_filter_events)

    # endregion

    # region CREATE LOG AND CONNECT TO EVENTS

    def create_log(self):
        if self.semantic_header.log.include:
            self._create_log_node()
            self._write_message_to_performance(message="Creation of (:Log) nodes")

            if self.semantic_header.log.has:
                self._link_events_to_log()
                self._write_message_to_performance(message="Creation of (:Event) <- [:HAS] - (:Log) relation")

    def _create_log_node(self):
        q_create_log = f'''
                        MATCH (e:Event) WHERE e.log IS NOT NULL AND e.log <> "nan"
                        WITH e.log AS log
                        MERGE (:Log {{ID:log}})
                    '''

        self.exec_query(q_create_log)

    def _link_events_to_log(self):
        q_link_events_to_log = f'''
                            CALL apoc.periodic.iterate(
                                'MATCH (l:Log) 
                                MATCH (e:Event {{log: l.ID}})
                                RETURN e, l', 
                                'MERGE (l)-[:HAS]->(e)',
                                {{batchSize:{self.batch_size}}})
                            '''

        self.exec_query(q_link_events_to_log)

    # endregion

    # region CREATE ENTITIES

    def create_entities(self) -> None:
        entity: EntityLPG
        for entity in self.semantic_header.entities_derived_from_events:
            if entity.include and entity.use_nodes and entity.based_on == "Event":
                self._create_entity(entity=entity)
                self._write_message_to_performance(f"Entity (:{entity.get_label_string()}) node created")

    def _create_entity(self, entity: EntityLPG) -> None:
        # find events that contain the entity as property and not nan
        # save the value of the entity property as id and also whether it is a virtual entity
        # create a new entity node if it not exists yet with properties

        conditions = entity.get_where_condition()
        composed_primary_id_query = entity.get_composed_primary_id()
        separate_primary_id_query = entity.get_entity_attributes()
        primary_key_properties = entity.get_entity_attributes_as_node_properties()

        entity_type = entity.type
        entity_labels_string = entity.get_label_string()

        q_create_entity = f'''
                    MATCH (e:{entity.based_on}) WHERE {conditions}
                    WITH {composed_primary_id_query} AS id, {separate_primary_id_query}
                    MERGE (en:{entity_labels_string}
                            {{ID:id, 
                            uID:"{entity_type}_"+toString(id),                    
                            entityType:"{entity_type}",
                            {primary_key_properties}}})
                    '''

        self.exec_query(q_create_entity)

    def correlate_events_to_entities(self) -> None:
        # correlate events that contain a reference from an entity to that entity node
        entities = self.semantic_header.entities_derived_from_events
        entity: EntityLPG
        for entity in entities:
            if entity.corr:
                # find events that contain the entity as property and not nan
                # save the value of the entity property as id and also whether it is a virtual entity
                # create a new entity node if it not exists yet with properties
                self._correlate_events_to_entity(entity=entity)

                self._write_message_to_performance(
                    f"Relation (:Event) - [:CORR] -> (:{entity.get_label_string()}) created")

    def _correlate_events_to_entity(self, entity: EntityLPG) -> None:
        # correlate events that contain a reference from an entity to that entity node
        entity_labels_string = entity.get_label_string()
        primary_key_id = entity.get_composed_primary_id()
        conditions = entity.get_where_condition()

        q_correlate = f'''
            CALL apoc.periodic.iterate(
                'MATCH (e:Event) WHERE {conditions}
                WITH e, {primary_key_id} as id
                MATCH (n:{entity_labels_string}) WHERE id = n.ID
                RETURN e, n',
                'MERGE (e)-[:CORR]->(n)',
                {{batchSize: {self.batch_size}}})
                '''
        self.exec_query(q_correlate)

    def correlate_events_to_derived_entity(self, derived_entity: str) -> None:
        # correlate events that are related to an entity which is reified into a new entity to the new reified entity
        q_correlate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{entityType:"{derived_entity}"}})
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{entityType:"{derived_entity}"}})
            MERGE (e)-[:CORR]->(r)'''
        self.exec_query(q_correlate)

    def create_entity_relations(self) -> None:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        relation: RelationLPG
        for relation in self.semantic_header.relations:
            if relation.include:
                self._create_entity_relationships(relation=relation)

                self._write_message_to_performance(
                    message=f"Relation (:{relation.from_node_label}) - [:{relation.type}] -> "
                            f"(:{relation.to_node_label}) done")

    def _create_entity_relationships(self, relation: RelationLPG) -> None:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        relation_type = relation.type
        entity_label_from_node = relation.from_node_label
        entity_label_to_node = relation.to_node_label
        foreign_key = relation.foreign_key

        q_create_relation = f'''
            MATCH (e1:Event) -[:CORR]-> (n1:{entity_label_to_node})
            MATCH (e2:Event) -[:CORR]-> (n2:{entity_label_from_node})
                WHERE n1 <> n2 AND e2.{foreign_key} = n1.ID
            WITH DISTINCT n1,n2
            MERGE (n1) <-[:{relation_type.upper()} {{type:"Rel",
                                                     {entity_label_to_node.lower()}Id: n1.ID,
                                                     {entity_label_from_node.lower()}Id: n2.ID                                                    
                                                    }}]- (n2)'''
        self.exec_query(q_create_relation)

    def reify_entity_relations_sh(self) -> None:
        relation: RelationLPG
        reified_entity: EntityLPG
        for reified_entity in self.semantic_header.reified_entities:
            if reified_entity.include:
                self._reify_entity_relations(reified_entity=reified_entity)
                self._write_message_to_performance(
                    message=f"Relation [:{reified_entity.type.upper()}] reified as (:Entity:{reified_entity.get_label_string()}) node")

    def _reify_entity_relations(self, reified_entity: EntityLPG) -> None:

        conditions = reified_entity.get_where_condition("r")
        composed_primary_id_query = reified_entity.get_composed_primary_id("r")
        separate_primary_id_query = reified_entity.get_entity_attributes("r")
        primary_key_properties = reified_entity.get_entity_attributes_as_node_properties()

        entity_type = reified_entity.type
        entity_labels_string = reified_entity.get_label_string()

        q_create_entity = f'''
                    MATCH (n1) - [r:{reified_entity.based_on}] -> (n2) WHERE {conditions}
                    WITH {composed_primary_id_query} AS id, {separate_primary_id_query}
                    MERGE (en:{entity_labels_string}
                            {{ID:id, 
                            uID:"{entity_type}_"+toString(id),                    
                            entityType:"{entity_type}",
                            {primary_key_properties}}})
                    MERGE (n1) <-[:REIFIED ] - (en) -[:REIFIED ]-> (n2)
                    '''
        # TODO FINISH
        q_correlate_entities = f'''
            MATCH (n1) - [r:{reified_entity.based_on}] -> (n2)
            MATCH (reified:{entity_labels_string}) 
        
        '''

        self.exec_query(q_create_entity)

        # entity_label_to_node = reified_entity.relation.to_node_label
        # entity_label_from_node = reified_entity.relation.from_node_label
        # relation_type = reified_entity.relation.type
        # reified_entity_type = reified_entity.type
        # reified_entity_labels_string = reified_entity.get_label_string()
        #
        # # create from a relation edge a new :relation node
        # # add a :REIFIED edge between the entities constituting this relationship and the new node
        # q_reify_relation = f'''
        #             MATCH (n1:{entity_label_to_node}) <- [rel:{relation_type.upper()}]- (n2:{entity_label_from_node})
        #             MERGE (new:Entity:{reified_entity_labels_string} {{
        #                 {entity_label_to_node}: n1.ID,
        #                 {entity_label_from_node}: n2.ID,
        #                 ID:toString(n1.ID)+"_"+toString(n2.ID),
        #                 entityType: "{reified_entity_type}",
        #                 uID:"{reified_entity_type}_"+toString(n1.ID)+"_"+toString(n2.ID)}})
        #             MERGE (n1) <-[:REIFIED ] - (new) -[:REIFIED ]-> (n2)'''
        # self.exec_query(q_reify_relation)

    def correlate_events_to_reification(self) -> None:
        reified_entity: EntityLPG
        for reified_entity in self.semantic_header.reified_entities:
            if reified_entity.corr:
                reified_entity_labels = reified_entity.get_label_string()
                # correlate events that are related to an entity which is reified into a new entity
                # to the new reified entity
                q_correlate = f'''
                        MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity:{reified_entity_labels})
                        MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity:{reified_entity_labels})
                        MERGE (e)-[:CORR]->(r)'''
                self.exec_query(q_correlate)

                self._write_message_to_performance(
                    f"Relation (:Event) - [:CORR] -> (:Entity:{reified_entity_labels}) created")

    # endregion

    # region CREATE DIRECTLY FOLLOWS RELATIONS

    def create_df_edges(self) -> None:
        entity: EntityLPG

        for entity in self.semantic_header.entities_derived_from_events:
            if entity.df:
                self._create_directly_follows(entity=entity)
                self._write_message_to_performance(f"Created [:DF] edge for (:{entity.get_label_string()})")

        for entity in self.semantic_header.reified_entities:
            if entity.df:
                self._create_directly_follows(entity=entity)
                self._write_message_to_performance(
                    f"Created [:DF] edge for (:{entity.get_label_string()})")

    def _create_directly_follows(self, entity: EntityLPG) -> None:
        # find the specific entities and events with a certain label correlated to that entity
        # order all events by time, order_nr and id grouped by a node n
        # collect the sorted nodes as a list
        # unwind the list from 0 to the one-to-last node
        # find neighbouring nodes and add an edge between
        entity_type = entity.type
        entity_labels_string = entity.get_label_string()

        df_entity_string = entity.get_df_label()

        q_create_df = f'''
         CALL apoc.periodic.iterate(
            'MATCH (n:{entity_labels_string}) <-[:CORR]- (e)
            WITH n , e as nodes ORDER BY e.timestamp,e.order_nr, ID(e)
            WITH n , collect (nodes) as nodeList
            UNWIND range(0,size(nodeList)-2) AS i
            WITH n , nodeList[i] as first, nodeList[i+1] as second
            RETURN first, second',
            'MERGE (first) -[df:{df_entity_string} {{entityType: "{entity_type}"}}]->(second)
             SET df.type = "DF"
            ',
            {{batchSize: {self.batch_size}}})
        '''

        self.exec_query(q_create_df)

    def merge_duplicate_df(self):
        entity: EntityLPG
        for entity in self.semantic_header.entities_derived_from_events:
            if entity.merge_duplicate_df:
                self._merge_duplicate_df_entity(entity=entity)
                self.perf.finished_step(
                    activity=f"Merged duplicate [:DF] edges for (:{entity.get_label_string()}) done")

    def _merge_duplicate_df_entity(self, entity: EntityLPG) -> None:
        q_merge_duplicate_rel = f'''
                    MATCH (n1:Event)-[r:{entity.get_df_label()} {{entityType: "{entity.type}"}}]->(n2:Event)
                    WITH n1, n2, collect(r) AS rels
                    WHERE size(rels) > 1
                    // only include this and the next line if you want to remove the existing relationships
                    UNWIND rels AS r 
                    DELETE r
                    MERGE (n1)-[:{entity.get_df_label()} {{entityType: "{entity.type}", Count:size(rels), type:"DF"}}]->(n2)
                '''
        self.exec_query(q_merge_duplicate_rel)

    def delete_parallel_dfs_derived(self):
        reified_entity: EntityLPG
        original_entity: EntityLPG
        relation: RelationLPG
        for reified_entity in self.semantic_header.reified_entities:
            if reified_entity.delete_parallel_df:
                relation = reified_entity.relation
                parent_entity = self.semantic_header.get_entity(relation.from_node_label)
                child_entity = self.semantic_header.get_entity(relation.to_node_label)
                for original_entity in [parent_entity, child_entity]:
                    self._delete_parallel_directly_follows_derived(
                        reified_entity=reified_entity,
                        original_entity=original_entity)
                    self._write_message_to_performance(
                        f"Deleted parallel DF of (:{reified_entity.get_label_string()}) and (:{original_entity.get_label_string()})")

    def _delete_parallel_directly_follows_derived(self, reified_entity: EntityLPG, original_entity: EntityLPG):
        reified_entity_type = reified_entity.type
        df_reified_entity = reified_entity.get_df_label()

        original_entity_type = original_entity.type
        df_original_entity = original_entity.get_df_label()

        q_delete_df = f'''
            MATCH (e1:Event) -[df:{df_reified_entity} {{entityType: "{reified_entity_type}"}}]-> (e2:Event)
            WHERE (e1:Event) -[:{df_original_entity} {{entityType: "{original_entity_type}"}}]-> (e2:Event)
            DELETE df'''

        self.exec_query(q_delete_df)

        self._write_message_to_performance(
            f"Deleted parallel DF between {reified_entity_type} and {original_entity_type}")

    def aggregate_df_relations(self, entity: EntityLPG = None,
                               include_label_in_c_df: bool = True,
                               classifiers: Optional[List[str]] = None, df_threshold: int = 0,
                               relative_df_threshold: float = 0) -> None:
        # add relations between classes when desired
        if entity is None or classifiers is None:
            q_create_dfc = f'''
                       MATCH (c1:Class) <-[:OBSERVED]- (e1:Event) -[df]-> (e2:Event) -[:OBSERVED]-> (c2:Class)
                       MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                       WHERE c1.type = c2.type AND n.entityType = df.entityType
                       WITH n.entityType as EType,c1 AS c1,count(df) AS df_freq,c2 AS c2
                       MERGE (c1) -[rel2:DF_C {{entityType:EType, type:"DF_C"}}]-> (c2) ON CREATE SET rel2.count=df_freq
                       '''

            q_change_label = f'''
                    MATCH (c1) -[rel2:DF_C]-> (c2) 
                    WITH rel2, rel2.entityType as EType, rel2.count AS df_freq, c1, c2
                       CALL apoc.do.when(
                        {include_label_in_c_df},
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

        elif df_threshold == 0 and relative_df_threshold == 0:
            # corresponds to aggregate_df_relations &  aggregate_df_relations_for_entities in graphdb-event-logs
            # aggregate only for a specific entity type and event classifier
            classifier_string = "_".join(classifiers)
            df_label = entity.get_df_label()
            entity_type = entity.type
            dfc_label = EventKnowledgeGraph.get_dfc_label(entity_type, include_label_in_c_df)
            q_create_dfc = f'''
                        MATCH (c1:Class) <-[:OBSERVED]- (e1:Event) -[df:{df_label} {{entityType: '{entity_type}'}}]-> 
                            (e2:Event) -[:OBSERVED]-> (c2:Class)
                        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                        WHERE n.entityType = df.entityType AND 
                            c1.type = "{classifier_string}" AND c2.type="{classifier_string}"
                        WITH n.entityType as EType,c1,count(df) AS df_freq,c2
                        MERGE (c1) -[rel2:{dfc_label} {{entityType: '{entity_type}', type:"DF_C"}}]-> (c2) 
                        ON CREATE SET rel2.count=df_freq'''
            self.exec_query(q_create_dfc)
        else:
            # aggregate only for a specific entity type and event classifier
            # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
            classifier_string = "_".join(classifiers)
            df_label = entity.get_df_label()
            entity_type = entity.type
            dfc_label = EventKnowledgeGraph.get_dfc_label(entity_type, include_label_in_c_df)
            q_create_dfc = f'''
                        MATCH (c1:Class) <-[:OBSERVED]- (e1:Event) -[df:{df_label} {{entityType: '{entity_type}'}}]-> 
                            (e2:Event) -[:OBSERVED]-> (c2:Class)
                        MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                        WHERE n.entityType = df.entityType 
                            AND c1.type = "{classifier_string}" AND c2.type="{classifier_string}"
                        WITH n.entityType as entityType,c1,count(df) AS df_freq,c2
                        WHERE df_freq > {df_threshold}
                        OPTIONAL MATCH (c2:Class) <-[:OBSERVED]- (e2b:Event) -[df2:DF]-> 
                            (e1b:Event) -[:OBSERVED]-> (c1:Class)
                        WITH entityType as EType,c1,df_freq,count(df2) AS df_freq2,c2
                        WHERE (df_freq*{relative_df_threshold} > df_freq2)
                        MERGE (c1) -[rel2:{dfc_label} {{entityType: '{entity_type}', type:"DF_C"}}]-> (c2) 
                        ON CREATE SET rel2.count=df_freq'''
            self.exec_query(q_create_dfc)

    # endregion

    # region CREATE CLASSES
    def create_classes(self):
        classes = self.semantic_header.classes
        for _class in classes:
            label = _class.label
            required_attributes = _class.class_identifiers
            ids = _class.ids

            self.create_class(label, required_attributes, ids)

    def create_class(self, label: str, required_keys: Optional[List[str]],
                     ids: Optional[List[str]]) -> None:
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
        _id = "+".join([f"{key}" for key in required_keys])
        class_label = "_".join([f"{key}" for key in required_keys])
        # add to the keys
        required_keys = [_id] + required_keys

        node_properties = ', '.join([f"{_id}: {key}" for _id, key in zip(ids, required_keys)])
        node_properties += f", type: '{_id}'"  # save ID also as string that captures the type

        # create new class nodes for event nodes that match the condition
        q_create_ec = f'''
                    MATCH (e:{label})
                    WHERE {where_condition_not_null}
                    WITH distinct {with_condition}
                    MERGE (c:Class:Class_{class_label} {{ {node_properties} }})'''
        self.exec_query(q_create_ec)

        # reformat to e.key
        required_keys = [f"e.{key}" for key in required_keys]
        where_link_condition = ' AND '.join([f"c.{_id} = {key}" for _id, key in zip(ids[1:], required_keys[1:])])

        # Create :OBSERVED relation between the class and events
        q_link_event_to_class = f'''
                CALL apoc.periodic.iterate(
                    'MATCH (c:Class_{class_label})
                    MATCH (e:Event) WHERE {where_link_condition}
                    RETURN e, c',
                    'MERGE (e) -[:OBSERVED]-> (c)',
                    {{batchSize: {self.batch_size}}})                
                '''
        self.exec_query(q_link_event_to_class)

    # endregion

    # region CREATE STATIC NODES AND RELATIONS
    def create_static_nodes_and_relations(self):
        # TODO no implementation yet (see if needed)
        self._write_message_to_performance("Static Nodes and Relations are created")

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

    # endregion

    def print_statistics(self):
        node_count = self.get_node_count()
        edge_count = self.get_edge_count()
        agg_edge_count = self.get_aggregated_edge_count()
        result = node_count + edge_count + agg_edge_count
        print(tabulate(result))

    def get_node_count(self):
        query_count_nodes = """
                    // List all node types and counts
                        MATCH (n) 
                        WITH n, CASE labels(n)[0]
                            WHEN 'Event' THEN 0
                            WHEN 'Entity' THEN 1
                            WHEN 'Class' THEN 2
                            WHEN 'Log' THEN 3
                            ELSE 4
                        END as sortOrder
                        WITH  labels(n)[0] as label,  count(n) as numberOfNodes,sortOrder
                        RETURN label,  numberOfNodes ORDER BY sortOrder
                """

        result = self.exec_query(query_count_nodes)
        return result

    def get_edge_count(self):
        query_count_relations = """
            // List all agg rel types and counts
            MATCH () - [r] -> ()
            WHERE r.type is NOT NULL
            WITH toUpper(r.type) as type, count(r) as numberOfRelations
            RETURN type, numberOfRelations 
        """

        result = self.exec_query(query_count_relations)
        return result

    def get_aggregated_edge_count(self):
        query_count_relations = """
            // List all rel types and counts
            MATCH () - [r] -> ()
            WHERE r.type is  NULL
            WITH r, CASE Type(r)
              WHEN 'CORR' THEN 0
              WHEN 'OBSERVED' THEN 1
              WHEN 'HAS' THEN 2
              ELSE 3
            END as sortOrder
            
            WITH Type(r) as type, count(r) as numberOfRelations, sortOrder
            RETURN type, numberOfRelations ORDER BY sortOrder
        """

        result = self.exec_query(query_count_relations)
        return result
