from typing import Tuple, Dict, List, Union, Sequence
from pandas import DataFrame

from EventKnowledgeGraph import EventKnowledgeGraph

# several steps of import, each can be switch on/off
from performance_handling import Performance
from colorama import Fore

import authentication
from datasets import datasets, BPICNames

connection = authentication.connections_map[authentication.Connections.LOCAL]
dataset = datasets[BPICNames.BPIC16_FULL]

use_preloaded_files = False  # if false, read/import files instead
verbose = False


def create_graph_instance() -> EventKnowledgeGraph:
    """
    Creates an instance of an EventKnowledgeGraph
    @return: EventKnowledgeGraph
    """

    return EventKnowledgeGraph(db_name=connection.user, uri=connection.uri, user=connection.user,
                               password=connection.password, batch_size=5000,
                               option_df_entity_type_in_label=dataset.settings.option_df_entity_type_in_label,
                               verbose=verbose)


def clear_graph(graph: EventKnowledgeGraph, perf: Performance) -> None:
    """
    # delete all nodes and relations in the graph to start fresh
    @param graph: CypherGraph
    @return: None
    """

    print("Clearing DB...")
    graph.clear_db(db_name = connection.user)
    perf.finished_step(activity=f"Database cleared", log_message=f"Cleared Database")


def populate_graph(file_name: str, graph: EventKnowledgeGraph, perf: Performance):
    semantics = dataset.semantics
    settings = dataset.settings

    """ STEP C1: IMPORT STATIC NODES"""
    graph.create_static_nodes_and_relations()

    """  STEP C3: IMPORT EVENT NODES"""
    # import the events from all sublogs in the graph with the corresponding labels
    if settings.step_load_events_from_csv:
        for file_name in dataset.file_names:
            graph.create_events(input_path=dataset.data_path, file_name=file_name, na_values=dataset.na_values, dtype_dict=dataset.dtype_dict)
            perf.finished_step(activity=f"Imported events from event log", log_message=f"Event nodes for {file_name} done")

    graph.set_constraints()
    perf.finished_step(activity=f"Set constraints", log_message=f"Constraints are set")

    if settings.step_filter_events:
        for attribute_value_pairs in semantics.properties_of_events_to_be_filtered:
            prop = attribute_value_pairs.attribute
            values = attribute_value_pairs.values
            graph.filter_events_by_property(prop=prop, values=values)
            perf.finished_step(activity=f"Events are filtered on {prop}", log_message=f"Events are filtered on {prop}")

    if settings.step_create_log:
        graph.create_log()
        perf.finished_step(activity=f"Log nodes have been created and events nodes are related with [:HAS] relation",
                           log_message=f"Created Log nodes")

    """  STEP C4: CREATE ENTITIES"""
    # for each entity, we add the entity nodes to graph and correlate them to the correct events
    if settings.step_create_entities:
        for entity in semantics.model_entities:
            entity_label = entity.entity_label
            if entity_label in semantics.include_entities:
                property_name_id = entity.property_name_id
                properties = entity.properties
                additional_label = None
                graph.create_entity(property_name_id=property_name_id, entity_label=entity_label,
                                    additional_label=additional_label, properties=properties)
                # STEP 4: Correlate events to entities using explicit relation
                graph.correlate_events_to_entity(property_name_id=property_name_id, entity_label=entity_label,
                                                 properties=properties)
                perf.finished_step(activity=f"Create_entity {entity_label}",
                                   log_message=f"Create Entity for '{entity_label}' done")

    if settings.step_create_entity_relations:
        for relation in semantics.model_relations:  # per relation
            relation_type = relation.relation_type
            entity_label_to_node = relation.entity_label_to_node
            entity_label_from_node = relation.entity_label_from_node
            reference_in_event_to_to_node = relation.reference_in_event_to_to_node

            graph.create_entity_relationships(relation_type=relation_type,
                                              entity_label_from_node=entity_label_from_node,
                                              entity_label_to_node=entity_label_to_node,
                                              reference_in_event_to_to_node=reference_in_event_to_to_node)
            perf.finished_step(activity=f"create_entity_relationships {relation_type}",
                               log_message=f"Relation (:{entity_label_from_node}) - [:{reference_in_event_to_to_node}] -> (:{entity_label_to_node})"
                                           f" done")

            if settings.step_reify_relations:
                derived_entity = relation_type
                if derived_entity in semantics.model_entities_derived and derived_entity in semantics.include_entities:
                    graph.reify_entity_relations(entity_name1=entity_label_to_node, entity_name2=entity_label_from_node,
                                                 derived_entity=derived_entity)
                    graph.correlate_events_to_derived_entity(derived_entity=derived_entity)
                    perf.finished_step(activity=f"Relation {relation_type} reified",
                                       log_message=f"Relation {relation_type} reified")
    else:
        perf.finished_step(activity=f"No Relations created",
                           log_message=f"No Relations created")

    if settings.step_create_df:
        for entity in semantics.include_entities:  # per entity
            graph.create_directly_follows(entity_name=entity)

            perf.finished_step(activity=f"create_df '{entity}'",
                               log_message=f"DF for Entity '{entity}' done")
    else:
        perf.finished_step(activity=f"No DF Relations created",
                       log_message=f"No DF Relations created")

    if settings.step_delete_parallel_df:
        for relation in semantics.model_relations:  # per relation
            derived_entity = relation.relation_type

            if derived_entity not in semantics.include_entities or \
                    derived_entity not in semantics.model_entities_derived:
                continue

            parent_entity = relation.entity_label_from_node
            child_entity = relation.entity_label_to_node

            graph.delete_parallel_directly_follows_derived(derived_entity_type=derived_entity,
                                                           original_entity_type=parent_entity)

            perf.finished_step(activity=f"Deleted parallel DF between {derived_entity} and {parent_entity}'",
                               log_message=f"Deleted parallel DF between {derived_entity} and {parent_entity}")
            graph.delete_parallel_directly_follows_derived(derived_entity_type=derived_entity,
                                                           original_entity_type=child_entity)


            perf.finished_step(activity=f"Deleted parallel DF between {derived_entity} and {child_entity}'",
                               log_message=f"Deleted parallel DF between {derived_entity} and {child_entity}")

    if settings.step_delete_duplicate_df:
        for entity in semantics.include_entities:  # per entity
            graph.merge_duplicate_df(entity_name=entity)

            perf.finished_step(activity=f"Merged df '{entity}'",
                               log_message=f"Duplicate DF for Entity '{entity}' are merged")

    if settings.step_create_event_classes:
        classes = semantics.classes
        for _class in classes:
            label = _class.label
            required_keys = _class.required_keys
            ids = _class.ids
            graph.create_class(label=label, required_keys=required_keys, ids=ids)

            perf.finished_step(activity=f"Created classes for {label}'",
                               log_message=f"Created classes for {label}")
    else:
        perf.finished_step(activity=f"No classes created'",
                           log_message=f"No classes created")


    if settings.step_create_dfc:
        for dfc_entity in semantics.dfc_entities:
            classifiers = dfc_entity.classifiers
            entities = dfc_entity.entities
            for entity in entities:
                graph.aggregate_df_relations(entity_type=entity, classifiers=classifiers)


def main() -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    if use_preloaded_files:
        print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
    else:
        print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

    perf_file_path = dataset.perf_file_path
    perf_file_name = dataset.perf_file_name

    # performance class to measure performance
    perf = Performance(perf_file_path, perf_file_name, number_of_steps=dataset.number_of_steps)
    graph = create_graph_instance()

    # # create all entities
    # entities = Entities(entity_csv_path)
    # relations = Relations(relation_csv_path)

    if dataset.settings.step_clear_db:
        clear_graph(graph=graph, perf=perf)

    if dataset.settings.step_populate_graph:
        populate_graph(file_name=dataset.file_names, graph=graph, perf=perf)

    perf.finish()
    perf.save()

    graph.close_connection()


if __name__ == "__main__":
    main()
