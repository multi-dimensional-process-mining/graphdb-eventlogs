from dataclasses import dataclass
from typing import Dict, Optional, Any, List

from csv_to_eventgraph_neo4j.datastructures import DataStructure
from csv_to_eventgraph_neo4j.semantic_header_lpg import EntityLPG, RelationLPG, ClassLPG
from string import Template


@dataclass
class Query:
    query_string: str
    kwargs: Optional[Dict[str, any]]


class CypherQueryLibrary:

    @staticmethod
    def get_event_label(label: str, properties: Optional[Dict[str, Any]] = None, option_event_type_in_label=False):
        """
        Create the df label based on self.option_DF_entity_type_in_label
        If not in entity type, add it as property to the label
        @param label: str, label that should be created in the DF
        @param properties:
        @return:
        """
        if properties is None:
            if option_event_type_in_label:
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

            if option_event_type_in_label:
                return f'{label} {{{conditions}}}'
            else:
                return f'Event {{EventType: "{label}", {conditions}}} '

    @staticmethod
    def get_dfc_label(entity_type: str, include_label_in_dfc: bool) -> str:
        if include_label_in_dfc:
            return f'DF_C_{entity_type.upper()}'
        else:
            return f'DF_C'

    @staticmethod
    def get_all_rel_types_query() -> Query:
        # find all relations and return the distinct types
        q_request_rel_types = '''
                    MATCH () - [rel] - () return DISTINCT type(rel) as rel_type
                    '''
        return Query(query_string=q_request_rel_types, kwargs={})

    @staticmethod
    def get_all_node_labels() -> Query:
        # find all nodes and return the distinct labels
        q_request_node_labels = '''
                    MATCH (n) return DISTINCT labels(n) as label
                    '''
        return Query(query_string=q_request_node_labels, kwargs={})

    @staticmethod
    def get_clear_db_query(db_name) -> Query:
        q_replace_database = f'''
                        CREATE OR REPLACE DATABASE {db_name}
                        WAIT
                    '''

        return Query(query_string=q_replace_database, kwargs={"database": "system"})

    @staticmethod
    def get_constraint_unique_event_id_query() -> Query:
        query_constraint_unique_event_id = f'''
                        CREATE CONSTRAINT unique_event_ids IF NOT EXISTS 
                        FOR (e:Event) REQUIRE e.ID IS UNIQUE'''
        return Query(query_string=query_constraint_unique_event_id, kwargs={})

    @staticmethod
    def get_constraint_unique_entity_uid_query() -> Query:
        query_constraint_unique_entity_uid = f'''
                                CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                                FOR (en:Entity) REQUIRE en.uID IS UNIQUE'''
        return Query(query_string=query_constraint_unique_entity_uid, kwargs={})

    @staticmethod
    def get_constraint_unique_log_id_query() -> Query:
        query_constraint_unique_log_id = f'''
                                CREATE CONSTRAINT unique_entity_ids IF NOT EXISTS 
                                FOR (l:Log) REQUIRE l.ID IS UNIQUE'''
        return Query(query_string=query_constraint_unique_log_id, kwargs={})

    @staticmethod
    def get_create_events_batch_query(batch: List[Dict[str, str]], labels: List[str]) -> Query:
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

        return Query(query_string=q_create_events_batch, kwargs={"batch": batch})

    @staticmethod
    def get_make_timestamp_date_query(attribute, datetime_object) -> Query:
        """
        Convert the strings of the timestamp to the datetime as used in cypher
        Remove the str_timestamp property
        @return: None
        """
        offset = datetime_object.timezone_offset
        offset = f"+'{offset}'" if offset != "" else offset

        q_make_timestamp = f'''
            CALL apoc.periodic.iterate(
            "MATCH (e:Event) WHERE e.{attribute} IS NOT NULL AND e.justImported = True 
            WITH e, e.{attribute}{offset} as timezone_dt
            WITH e, datetime(apoc.date.convertFormat(timezone_dt, '{datetime_object.format}', 
                '{datetime_object.convert_to}')) as converted
            RETURN e, converted",
            "SET e.{attribute} = converted",
            {{batchSize:10000, parallel:false}})
        '''

        return Query(query_string=q_make_timestamp, kwargs={})

    @staticmethod
    def get_finalize_import_events_query(labels) -> Query:
        labels = ":".join(labels)
        q_set_just_imported_to_false = f'''
        CALL apoc.periodic.iterate(
            "MATCH (e:{labels}) WHERE e.justImported = True 
            RETURN e",
            "REMOVE e.justImported",
            {{batchSize:10000, parallel:false}})
        '''

        return Query(query_string=q_set_just_imported_to_false, kwargs={})

    @staticmethod
    def get_filter_events_by_property_query(prop: str, values: Optional[List[str]] = None, exclude=True) -> Query:
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
        return Query(query_string=q_filter_events, kwargs={})

    @staticmethod
    def get_create_log_query() -> Query:
        q_create_log = f'''
                            MATCH (e:Event) WHERE e.log IS NOT NULL AND e.log <> "nan"
                            WITH e.log AS log
                            MERGE (:Log {{ID:log}})
                        '''
        return Query(query_string=q_create_log, kwargs={})

    @staticmethod
    def get_link_events_to_log_query(batch_size) -> Query:
        q_link_events_to_log = f'''
                                CALL apoc.periodic.iterate(
                                    'MATCH (l:Log) 
                                    MATCH (e:Event {{log: l.ID}})
                                    RETURN e, l', 
                                    'MERGE (l)-[:HAS]->(e)',
                                    {{batchSize:{batch_size}}})
                                '''
        return Query(query_string=q_link_events_to_log, kwargs={})

    @staticmethod
    def get_create_entity_query(entity: EntityLPG) -> Query:
        # find events that contain the entity as property and not nan
        # save the value of the entity property as id and also whether it is a virtual entity
        # create a new entity node if it not exists yet with properties

        conditions = entity.get_where_condition()
        composed_primary_id_query = entity.get_composed_primary_id()
        attribute_properties_with_statement = entity.get_entity_attributes()
        entity_attributes = entity.get_entity_attributes_as_node_properties()

        entity_type = entity.type
        entity_labels_string = entity.get_label_string()

        q_create_entity = f'''
                    MATCH (e:{entity.based_on}) WHERE {conditions}
                    WITH {composed_primary_id_query} AS id, {attribute_properties_with_statement}
                    WHERE id <> "Unknown"
                    MERGE (en:{entity_labels_string}
                            {{ID:id, 
                            uID:"{entity_type}_"+toString(id),                    
                            entityType:"{entity_type}",
                            {entity_attributes}}})
                    '''

        return Query(query_string=q_create_entity, kwargs={})

    @staticmethod
    def get_correlate_events_to_entity_query(entity: EntityLPG, batch_size: int) -> Query:
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
                {{batchSize: {batch_size}}})
                '''

        return Query(query_string=q_correlate, kwargs={})

    @staticmethod
    def get_correlate_events_to_derived_entity_query(derived_entity: str) -> Query:
        # correlate events that are related to an entity which is reified into a new entity to the new reified entity
        q_correlate = f'''
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{entityType:"{derived_entity}"}})
            MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity {{entityType:"{derived_entity}"}})
            MERGE (e)-[:CORR]->(r)'''

        return Query(query_string=q_correlate, kwargs={})

    @staticmethod
    def get_create_entity_relationships_query(relation: RelationLPG) -> Query:
        # find events that are related to different entities of which one event also has a reference to the other entity
        # create a relation between these two entities
        relation_type = relation.type
        entity_label_from_node = relation.from_node_label
        entity_label_to_node = relation.to_node_label
        foreign_key = relation.foreign_key
        primary_key = relation.primary_key

        q_create_relation = f'''
            MATCH (_from:{entity_label_from_node})
            MATCH (to:{entity_label_to_node})
                WHERE to <> _from AND (to.{primary_key} in _from.{foreign_key} OR to.{primary_key} = _from.{foreign_key})
            WITH DISTINCT _from, to
            MERGE (_from) - [:{relation_type.upper()} {{type:"Rel",
                    {entity_label_from_node.lower()}Id: _from.ID,
                    {entity_label_to_node.lower()}Id: to.{primary_key}                                              
                                                    }}]-> (to)'''

        return Query(query_string=q_create_relation, kwargs={})

    @staticmethod
    def get_reify_entity_relations_query(reified_entity: EntityLPG) -> Query:

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
                    '''

        return Query(query_string=q_create_entity, kwargs={})

    @staticmethod
    def get_add_reified_query(reified_entity: EntityLPG, batch_size: int):
        conditions = reified_entity.get_where_condition("r")
        composed_primary_id_query = reified_entity.get_composed_primary_id("r")
        entity_labels_string = reified_entity.get_label_string()

        q_correlate_entities = f'''
            CALL apoc.periodic.iterate(
                'MATCH (n1) - [r:{reified_entity.based_on}] -> (n2) WHERE {conditions}
                WITH n1, n2, {composed_primary_id_query} AS id
                MATCH (reified:{entity_labels_string}) WHERE id = reified.ID
                RETURN n1, n2, reified',
                'MERGE (n1) <-[:REIFIED ] - (reified) -[:REIFIED ]-> (n2)',
                {{batchSize: {batch_size}}})

        '''

        return Query(query_string=q_correlate_entities, kwargs={})

    @staticmethod
    def get_correlate_events_to_reification_query(reified_entity: EntityLPG):
        reified_entity_labels = reified_entity.get_label_string()
        q_correlate = f'''
                                MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity:{reified_entity_labels})
                                MATCH (e:Event) -[:CORR]-> (n:Entity) <-[:REIFIED ]- (r:Entity:{reified_entity_labels})
                                MERGE (e)-[:CORR]->(r)'''
        return Query(query_string=q_correlate, kwargs={})

    @staticmethod
    def get_create_directly_follows_query(entity: EntityLPG, batch_size) -> Query:
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
            WITH n , e as nodes ORDER BY e.timestamp, ID(e)
            WITH n , collect (nodes) as nodeList
            UNWIND range(0,size(nodeList)-2) AS i
            WITH n , nodeList[i] as first, nodeList[i+1] as second
            RETURN first, second',
            'MERGE (first) -[df:{df_entity_string} {{entityType: "{entity_type}"}}]->(second)
             SET df.type = "DF"
            ',
            {{batchSize: {batch_size}}})
        '''

        return Query(query_string=q_create_df, kwargs={})

    @staticmethod
    def get_merge_duplicate_df_entity_query(entity: EntityLPG) -> Query:
        q_merge_duplicate_rel = f'''
                    MATCH (n1:Event)-[r:{entity.get_df_label()} {{entityType: "{entity.type}"}}]->(n2:Event)
                    WITH n1, n2, collect(r) AS rels
                    WHERE size(rels) > 1
                    // only include this and the next line if you want to remove the existing relationships
                    UNWIND rels AS r 
                    DELETE r
                    MERGE (n1)-[:{entity.get_df_label()} {{entityType: "{entity.type}", Count:size(rels), type:"DF"}}]->(n2)
                '''
        return Query(query_string=q_merge_duplicate_rel, kwargs={})

    @staticmethod
    def delete_parallel_directly_follows_derived(reified_entity: EntityLPG, original_entity: EntityLPG):
        reified_entity_type = reified_entity.type
        df_reified_entity = reified_entity.get_df_label()

        original_entity_type = original_entity.type
        df_original_entity = original_entity.get_df_label()

        q_delete_df = f'''
            MATCH (e1:Event) -[df:{df_reified_entity} {{entityType: "{reified_entity_type}"}}]-> (e2:Event)
            WHERE (e1:Event) -[:{df_original_entity} {{entityType: "{original_entity_type}"}}]-> (e2:Event)
            DELETE df'''

        return Query(query_string=q_delete_df, kwargs={})

    @staticmethod
    def _get_aggregate_df_relations_query(entity: EntityLPG = None,
                                          include_label_in_c_df: bool = True,
                                          classifiers: Optional[List[str]] = None, df_threshold: int = 0,
                                          relative_df_threshold: float = 0) -> List[Query]:
        # add relations between classes when desired
        # TODO: split queries
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

            return [Query(query_string=q_create_dfc, kwargs={}), Query(query_string=q_change_label, kwargs={})]

        elif df_threshold == 0 and relative_df_threshold == 0:
            # corresponds to aggregate_df_relations &  aggregate_df_relations_for_entities in graphdb-event-logs
            # aggregate only for a specific entity type and event classifier
            classifier_string = "_".join(classifiers)
            df_label = entity.get_df_label()
            entity_type = entity.type
            dfc_label = CypherQueryLibrary.get_dfc_label(entity_type, include_label_in_c_df)
            q_create_dfc = f'''
                            MATCH (c1:Class) <-[:OBSERVED]- (e1:Event) -[df:{df_label} {{entityType: '{entity_type}'}}]-> 
                                (e2:Event) -[:OBSERVED]-> (c2:Class)
                            MATCH (e1) -[:CORR] -> (n) <-[:CORR]- (e2)
                            WHERE n.entityType = df.entityType AND 
                                c1.type = "{classifier_string}" AND c2.type="{classifier_string}"
                            WITH n.entityType as EType,c1,count(df) AS df_freq,c2
                            MERGE (c1) -[rel2:{dfc_label} {{entityType: '{entity_type}', type:"DF_C"}}]-> (c2) 
                            ON CREATE SET rel2.count=df_freq'''
            return [Query(query_string=q_create_dfc, kwargs={})]
        else:
            # aggregate only for a specific entity type and event classifier
            # include only edges with a minimum threshold, drop weak edges (similar to heuristics miner)
            classifier_string = "_".join(classifiers)
            df_label = entity.get_df_label()
            entity_type = entity.type
            dfc_label = CypherQueryLibrary.get_dfc_label(entity_type, include_label_in_c_df)
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
            return [Query(query_string=q_create_dfc, kwargs={})]

    @staticmethod
    def get_create_class_query(_class: ClassLPG) -> Query:
        # make sure first element of id list is cID
        label = _class.label

        group_by = _class.get_group_by_statement(node_name="e")
        where_condition_not_null = _class.get_condition(node_name="e")
        class_properties = _class.get_class_properties()
        class_label = _class.get_class_label()

        # create new class nodes for event nodes that match the condition
        q_create_ec = f'''
                    MATCH (e:{label})
                    WHERE {where_condition_not_null}
                    WITH {group_by}
                    MERGE (c:Class:Class_{class_label} {{ {class_properties} }})'''

        return Query(query_string=q_create_ec, kwargs={})

    @staticmethod
    def get_link_event_to_class_query(_class: ClassLPG, batch_size: int) -> Query:
        # Create :OBSERVED relation between the class and events
        class_label = _class.get_class_label()
        where_link_condition = _class.get_link_condition(class_node_name="c", event_node_name="e")

        q_link_event_to_class = f'''
                CALL apoc.periodic.iterate(
                    'MATCH (c:Class_{class_label})
                    MATCH (e:Event) WHERE {where_link_condition}
                    RETURN e, c',
                    'MERGE (e) -[:OBSERVED]-> (c)',
                    {{batchSize: {batch_size}}})                
                '''

        return Query(query_string=q_link_event_to_class, kwargs={})

    @staticmethod
    def get_node_count_query() -> Query:
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

        return Query(query_string=query_count_nodes, kwargs={})

    @staticmethod
    def get_edge_count_query() -> Query:
        query_count_relations = """
                // List all agg rel types and counts
                MATCH () - [r] -> ()
                WHERE r.type is NOT NULL
                WITH toUpper(r.type) as type, count(r) as numberOfRelations
                RETURN type, numberOfRelations 
            """

        return Query(query_string=query_count_relations, kwargs={})

    @staticmethod
    def get_aggregated_edge_count_query() -> Query:
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

        return Query(query_string=query_count_relations, kwargs={})

    @staticmethod
    def merge_same_nodes(data_structure: DataStructure):
        query_str = '''
            MATCH (n:$labels)
            WITH $primary_keys, collect(n) as nodes
            CALL apoc.refactor.mergeNodes(nodes, {
                properties:"combine"})
            YIELD node
            RETURN node
        '''

        labels = ":".join(data_structure.labels)
        primary_keys = data_structure.get_primary_keys()
        primary_key_with = [f"n.{primary_key} as {primary_key}" for primary_key in primary_keys]
        primary_key_string = ", ".join(primary_key_with)

        query_str = Template(query_str).substitute(labels=labels, primary_keys=primary_key_string)

        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def add_attributes_to_classifier(relation, label, properties, copy_as):
        if copy_as is None:
            copy_as = properties

        query_str = '''
                    MATCH (c:Class) - [:$relation] - (n:$label)
                    SET $properties
                '''
        properties = [f"c.{copy} = n.{property}" for (property, copy) in zip(properties, copy_as)]
        properties = ",".join(properties)

        query_str = Template(query_str).substitute(relation=relation, label=label, properties=properties)

        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def infer_items_to_load_events(entity: EntityLPG, is_load=True) -> Query:
        query_str = '''
            MATCH (e:Event) - [:CORR] -> (n:$entity)
            MATCH (e) - [:CORR] ->  (equipment:Equipment)
            MATCH (e) - [:OBSERVED] -> (:Class) - [:AT] - (:Location) - [:PART_OF*0..] -> (l:Location) 
            MATCH (l) - [:AT] - (c:Class  {type: "physical", subtype: "$subtype", entity: "$entity"})
            WITH e, c, equipment, n
            CALL {WITH e, c, equipment
                MATCH (load_event:Event) - [:OBSERVED] -> (c) 
                MATCH (load_event) - [:CORR] ->  (equipment)
                WHERE load_event.timestamp $comparison e.timestamp AND load_event.$entity_id = "Unknown"
                RETURN load_event as $load_event_type
                ORDER BY load_event.timestamp $order_type
                LIMIT 1}
            MERGE ($load_event_type) - [:CORR] -> (n)
            '''

        subtype = "load" if is_load else "unload"
        load_event_type = "load_event_first_preceding" if is_load else "unload_event_first_successive"
        order_type = "DESC" if is_load else ""
        comparison = "<=" if is_load else ">="
        query_str = Template(query_str).substitute(entity=entity.type, entity_id=entity.get_primary_keys()[0],
                                                   subtype=subtype, comparison=comparison,
                                                   load_event_type=load_event_type,
                                                   order_type=order_type)

        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def infer_items_to_events_with_batch_position(entity: EntityLPG) -> Query:
        query_str = '''
        MATCH (e:Event) - [:CORR] -> (b:BatchPosition)
        WHERE e.$entity_id = "Unknown"
        MATCH (e) - [:CORR] -> (equipment:Equipment)
        MATCH (e) - [:OBSERVED] -> (:Class) - [:AT] - (:Location) - [:PART_OF*0..] -> (l:Location) 
        MATCH (l) - [:AT] - (c:Class {type: "physical", subtype: "load", entity: "$entity"})
        WITH e, c, equipment, b
        CALL {  WITH e, c, equipment, b
                MATCH (load_event:Event) - [:OBSERVED] -> (c) 
                MATCH (load_event) - [:CORR] -> (equipment)
                MATCH (load_event) - [:CORR] -> (n:$entity) - [:AT_POS] -> (b)
                WHERE load_event.timestamp <= e.timestamp
                RETURN load_event as load_event_inf, n
                ORDER BY load_event.timestamp DESC
                LIMIT 1
                }
        MERGE (e) - [:CORR] -> (n)
        '''

        query_str = Template(query_str).substitute(entity=entity.type, entity_id=entity.get_primary_keys()[0])

        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def infer_items_to_administrative_events_using_location(entity: EntityLPG) -> Query:
        query_str = '''
                    MATCH (e:Event) - [:CORR] -> (equipment:Equipment)
                    WHERE e.$entity_id = "Unknown"
                    MATCH (e) - [:OBSERVED] -> (:Class {type:"administrative"}) <- [:AT] - (l:Location)
                    MATCH (l) - [:AT] - (c:Class {subtype: "load", entity: "$entity"}) 
                    WITH e, equipment, c
                    CALL {  WITH e, equipment, c
                            MATCH (load_event:Event) - [:OBSERVED] -> (c)
                            MATCH (load_event) - [:CORR] -> (equipment)
                            MATCH (load_event) - [:CORR] -> (n:$entity)
                            WHERE load_event.timestamp <= e.timestamp
                            RETURN load_event as load_event_inf, n
                            ORDER BY load_event.timestamp DESC
                            LIMIT 1
                            }
                    MERGE (e) - [:CORR] -> (n)
                    '''

        query_str = Template(query_str).substitute(entity=entity.type, entity_id=entity.get_primary_keys()[0])

        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def add_entity_to_event(entity: EntityLPG) -> Query:
        query_str = '''
            MATCH (e:Event) - [:CORR] -> (n:$entity)
            WITH e, collect(n.ID) as related_entities_collection
            CALL{   WITH related_entities_collection
                    RETURN
                    CASE size(related_entities_collection)
                    WHEN 1 THEN related_entities_collection[0]
                    ELSE apoc.text.join(related_entities_collection, ',') 
                    END AS related_entities
                }
            SET e.$entity_id = related_entities
        '''

        query_str = Template(query_str).substitute(entity=entity.type, entity_id=entity.get_primary_keys()[0])

        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def match_entity_with_batch_position(entity: EntityLPG):
        query_str = '''
                MATCH (e:Event) - [:CORR] -> (n:$entity)
                MATCH (e) - [:CORR] -> (b:BatchPosition)
                MERGE (n) - [:AT_POS] -> (b)
            '''

        query_str = Template(query_str).substitute(entity=entity.type)
        return Query(query_string=query_str, kwargs={})

    @staticmethod
    def match_event_with_batch_position(entity: EntityLPG):
        query_str = '''
                   MATCH (e:Event) - [:CORR] -> (n:$entity) - [:AT_POS] -> (b:BatchPosition)
                   MERGE (e) - [:CORR] -> (b)
               '''

        query_str = Template(query_str).substitute(entity=entity.type)
        return Query(query_string=query_str, kwargs={})
