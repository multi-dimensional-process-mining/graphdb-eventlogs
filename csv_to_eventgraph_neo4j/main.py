from typing import Tuple, Dict, List, Union, Sequence
from pandas import DataFrame

from EventKnowledgeGraph import EventKnowledgeGraph

# several steps of import, each can be switch on/off
from performance_handling import Performance
from colorama import Fore

import authentication

is_sample = False
use_preloaded_files = False  # if false, read/import files instead
connection = authentication.connections_map[authentication.Connections.LOCAL_HOME_DESKTOP]

step_clear_db = True  # entire graph shall be cleared before starting a new import
step_populate_graph = True  # populate the graph
step_filter_events = True
step_createLog = True
step_createEntityRelations = True
step_reifyRelations = True
step_createDF = True
step_delete_parallel_df = False
step_delete_duplicate_df = True
step_create_event_classes = True
step_createDFC = True
step_createHOWnetwork = True

option_contains_lifecycle_information = True

model_entities_derived = ['Case_AO',
                          'Case_AW',
                          'Case_WO']

include_entities = ['Application', 'Workflow', 'Offer', 'Case_R', 'Case_AO', 'Case_AW', 'Case_WO']
# include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO','Case_AWO']

step_add_classes = True
step_statistics = False  # calculate some statisctics
step_perform_checks = False  # perform some statistical tests
step_add_mechanism = False
step_add_type_to_df = False
verbose = False

# won't clear the graph if it is not populated again
step_clear_db = False if step_populate_graph == False else step_clear_db

step_create_log_node = False  # create log nodes and relate events to log node
option_df_entity_type_in_label = True  # whether to include df entity type in label

def create_graph_instance() -> EventKnowledgeGraph:
    """
    Creates an instance of an EventKnowledgeGraph
    @return: EventKnowledgeGraph
    """

    return EventKnowledgeGraph(uri=connection.uri, user=connection.user, password=connection.password,
                               batch_size=10000, option_df_entity_type_in_label=option_df_entity_type_in_label,
                               verbose=verbose)


def clear_graph(graph: EventKnowledgeGraph, perf: Performance) -> None:
    """
    # delete all nodes and relations in the graph to start fresh
    @param graph: CypherGraph
    @return: None
    """

    print("Clearing DB...")
    graph.clear_db()
    perf.finished_step(activity=f"Database cleared", log_message=f"Cleared Database")


def populate_graph(file_name: str, graph: EventKnowledgeGraph, perf: Performance):
    """ STEP C1: IMPORT STATIC NODES"""
    graph.create_static_nodes_and_relations()

    """  STEP C3: IMPORT EVENT NODES"""
    # import the events from all sublogs in the graph with the corresponding labels
    graph.create_events(input_path=connection.data_path + file_name)
    perf.finished_step(activity=f"Imported events from event log", log_message=f"Event nodes for {file_name} done")

    graph.set_constraints()
    perf.finished_step(activity=f"Set constraints", log_message=f"Constraints are set")

    if step_filter_events:
        graph.filter_events_by_property(prop='lifecycle', values=["SUSPEND", "RESUME"])
        perf.finished_step(activity=f"Events are filtered on lifecycle", log_message=f"Events are filtered")

    if step_createLog:
        graph.create_log()
        perf.finished_step(activity=f"Log nodes have been created and events nodes are related with [:HAS] relation",
                           log_message=f"Created Log nodes")

    """  STEP C4: CREATE ENTITIES"""
    # for each entity, we add the entity nodes to graph and correlate them to the correct events
    print("Creating Entities")
    model_entities = [{'entity_label': 'Application',
                       'property_name_id': 'case',
                       'properties': {"EventOrigin": ['= "Application"']}},  # individual entities
                      {'entity_label': 'Workflow',
                       'property_name_id': 'case',
                       'properties': {"EventOrigin": ['= "Workflow"']}},
                      {'entity_label': 'Offer',
                       'property_name_id': 'OfferID',
                       'properties': {"EventOrigin": ['= "Offer"']}},
                      {'entity_label': 'Case_R',  # resource as entity
                       'property_name_id': 'resource',
                       'properties': {"resource": ["IS NOT NULL", '<> "nan"', '<> "None"']}},
                      {'entity_label': 'Case_AWO',  # original case notion
                       'property_name_id': 'case',
                       'properties': {"case": ["IS NOT NULL", '<> "nan"', '<> "None"']}}]

    # entities = [("Slot", None), (names.wafer, None), (names.split_lot, None),
    #             ("Connector", None),
    #             ("PairID", None), ("Blade", "Mechanism")]

    """  STEP C6: Correlate events to entities"""
    for entity in model_entities:
        entity_label = entity["entity_label"]
        property_name_id = entity["property_name_id"]
        properties = entity["properties"]
        additional_label = None
        graph.create_entity(property_name_id=property_name_id, entity_label=entity_label,
                            additional_label=additional_label, properties=properties)
        # STEP 4: Correlate events to entities using explicit relation
        graph.correlate_events_to_entity(property_name_id=property_name_id, entity_label=entity_label,
                                         properties=properties)
        perf.finished_step(activity=f"Create_entity {entity_label}",
                           log_message=f"Create Entity for '{entity_label}' done")

    model_relations = [
        {'relation_type': 'Case_AO',
         'entity_name1': 'Application',
         'entity_name2': 'Offer',
         'reference_from1to2': 'case'},
        {'relation_type': 'Case_AW',
         'entity_name1': 'Application',
         'entity_name2': 'Workflow',
         'reference_from1to2': 'case'},
        {'relation_type': 'Case_WO',
         'entity_name1': 'Workflow',
         'entity_name2': 'Offer',
         'reference_from1to2': 'case'}]

    if step_createEntityRelations:
        for relation in model_relations:  # per relation
            relation_type = relation["relation_type"]
            entity_name1 = relation["entity_name1"]
            entity_name2 = relation["entity_name2"]
            reference_from1to2 = relation["reference_from1to2"]

            graph.create_entity_relationships(relation_type=relation_type,
                                              entity_name1=entity_name1,
                                              entity_name2=entity_name2,
                                              reference_from1to2=reference_from1to2)
            perf.finished_step(activity=f"create_entity_relationships {relation_type}",
                               log_message=f"Relation (:{entity_name2}) - [:{reference_from1to2}] -> (:{entity_name1})"
                                           f" done")

            if step_reifyRelations:
                derived_entity = relation_type
                if derived_entity in model_entities_derived and derived_entity in include_entities:
                    graph.reify_entity_relations(entity_name1=entity_name1, entity_name2=entity_name2,
                                                 derived_entity=derived_entity)
                    graph.correlate_events_to_derived_entity(derived_entity=derived_entity)

    if step_createDF:
        for entity in include_entities:  # per entity
            graph.create_directly_follows(entity_name=entity)

            perf.finished_step(activity=f"create_df '{entity}'",
                               log_message=f"DF for Entity '{entity}' done")

    if step_delete_parallel_df:
        for relation in model_relations:  # per relation
            derived_entity = relation["relation_type"]

            if derived_entity not in include_entities or derived_entity not in model_entities_derived:
                continue

            parent_entity = relation["entity_name1"]
            child_entity = relation["entity_name2"]
            reference_from1to2 = relation["reference_from1to2"]

            graph.delete_parallel_directly_follows_derived(derived_entity_type=derived_entity,
                                                           original_entity_type=parent_entity)
            graph.delete_parallel_directly_follows_derived(derived_entity_type=derived_entity,
                                                           original_entity_type=child_entity)

    if step_delete_duplicate_df:
        for entity in include_entities:  # per entity
            graph.merge_duplicate_df(entity_name=entity)

            perf.finished_step(activity=f"Merged df '{entity}'",
                               log_message=f"Duplicate DF for Entity '{entity}' are merged")

    if step_create_event_classes:
        if option_contains_lifecycle_information:
            graph.create_class(label="Event", required_keys=["Activity", "lifecycle"], ids=["Name", "lifecycle"])
        else:
            graph.create_class(label="Event", required_keys=["Activity"], ids=["Name"])

    if step_createDFC:
        for entity in include_entities:
            if option_contains_lifecycle_information:
                classifier = "Activity_lifecycle"
            else:
                classifier = "Activity"
            graph.aggregate_df_relations(entity_type=entity, event_cl=classifier)
            # session.write_transaction(aggregateDFrelationsFiltering,entity,classifier,5000,3)
            # session.write_transaction(aggregateDFrelationsFiltering,entity,classifier,1,3)
            # session.write_transaction(aggregate_df_relations, entity, classifier)

    if step_createHOWnetwork:
        graph.create_class(label="Event", required_keys=["resource"], ids=["Name"])

        how_entities = include_entities
        how_entities.remove("Case_R")

        for entity in how_entities:
            graph.aggregate_df_relations(entity_type=entity, event_cl="resource")
        # "MATCH ( e : Event ) WITH distinct e.resource AS name \
        # MERGE ( c : Class {{ Name:name, Type:"Resource", ID: name}})'''"


def main() -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    if use_preloaded_files:
        print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
    else:
        print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

    if is_sample:
        file_name = 'BPIC17sample.csv'
        perf_file_name = 'BPIC17samplePerformance.csv'
    else:
        file_name = 'BPIC17full.csv'
        perf_file_name = 'BPIC17fullPerformance.csv'

    # performance class to measure performance
    perf = Performance(perf_file_name)
    graph = create_graph_instance()

    # # create all entities
    # entities = Entities(entity_csv_path)
    # relations = Relations(relation_csv_path)

    if step_clear_db:
        clear_graph(graph=graph, perf=perf)

    if step_populate_graph:
        populate_graph(file_name=file_name, graph=graph, perf=perf)

    # if step_add_classes:
    #     add_classes(graph=graph, perf=perf)

    if step_populate_graph or step_add_classes:
        # all actions are completed, thus the performance can be finished and saved
        perf.finish()
        perf.save()

    graph.close_connection()


if __name__ == "__main__":
    main()
