import json

from EventKnowledgeGraph import EventKnowledgeGraph, DatabaseConnection
from csv_to_eventgraph_neo4j.datastructures import ImportedDataStructures
from csv_to_eventgraph_neo4j.semantic_header_lpg import SemanticHeaderLPG

# several steps of import, each can be switch on/off
from performance_handling import Performance
from colorama import Fore

import authentication

connection = authentication.connections_map[authentication.Connections.LOCAL]

dataset_name = 'BPIC17'
use_sample = True

semantic_header = SemanticHeaderLPG.create_semantic_header(dataset_name)
perf_path = f"..\\perf\\{dataset_name}\\{dataset_name}Performance.csv"
number_of_steps = 100

datastructures = ImportedDataStructures(dataset_name)

step_clear_db = True
step_populate_graph = True
step_load_events_from_csv = True
step_filter_events = True
step_create_log = True
step_create_entities = True
step_create_entity_relations = True
step_reify_relations = True
step_create_df = True
step_delete_parallel_df = True
step_create_event_classes = True
step_create_dfc = True
step_delete_duplicate_df = True

use_preloaded_files = False  # if false, read/import files instead
verbose = False

db_connection = DatabaseConnection(db_name=connection.user, uri=connection.uri, user=connection.user,
                                   password=connection.password, verbose=verbose)


def create_graph_instance(perf: Performance) -> EventKnowledgeGraph:
    """
    Creates an instance of an EventKnowledgeGraph
    @return: returns an EventKnowledgeGraph
    """

    return EventKnowledgeGraph(db_connection=db_connection, db_name=connection.user,
                               batch_size=5000, event_tables=datastructures, use_sample=use_sample,
                               semantic_header=semantic_header, perf=perf)


def clear_graph(graph: EventKnowledgeGraph, perf: Performance) -> None:
    """
    # delete all nodes and relations in the graph to start fresh
    @param graph: EventKnowledgeGraph
    @param perf: Performance
    @return: None
    """

    print("Clearing DB...")
    graph.clear_db()
    perf.finished_step(log_message=f"Cleared DB")


def populate_graph(graph: EventKnowledgeGraph, perf: Performance):
    graph.create_static_nodes_and_relations()

    # import the events from all sublogs in the graph with the corresponding labels
    graph.import_data()
    perf.finished_step(log_message=f"(:Event) nodes done")

    # TODO: constraints in semantic header?
    graph.set_constraints()
    perf.finished_step(log_message=f"All constraints are set")

    graph.create_log()
    perf.finished_step(log_message=f"(:Log) nodes and [:HAS] relations done")

    # for each entity, we add the entity nodes to graph and correlate them to the correct events
    graph.create_entities()
    perf.finished_step(log_message=f"(:Entity) nodes done")

    graph.correlate_events_to_entities()
    perf.finished_step(log_message=f"[:CORR] edges done")

    graph.create_classes()
    perf.finished_step(log_message=f"(:Class) nodes done")

    graph.create_entity_relations()
    perf.finished_step(log_message=f"[:REL] edges done")

    graph.add_attributes_to_classifier(relation="IS", label="ActivityType", properties=["entity", "type", "subtype"])
    graph.add_attributes_to_classifier(relation="AT", label="Location", properties=["ID"], copy_as=["location"])

    graph.reify_entity_relations()
    perf.finished_step(log_message=f"Reified (:Entity) nodes done")

    graph.correlate_events_to_reification()
    perf.finished_step(log_message=f"[:CORR] edges for Reified (:Entity) nodes done")

    entity = semantic_header.get_entity("Box")
    graph.infer_items_to_load_events(entity=entity, is_load=True)
    graph.infer_items_to_load_events(entity=entity, is_load=False)
    graph.match_entity_with_batch_position(entity=entity)
    graph.infer_items_to_events_with_batch_position(entity=entity)
    graph.infer_items_to_administrative_events_using_location(entity=entity)
    graph.match_event_with_batch_position(entity=entity)
    entity = semantic_header.get_entity("BatchPosition")
    graph.add_entity_to_event(entity=entity)

    graph.create_df_edges()
    perf.finished_step(log_message=f"[:DF] edges done")

    graph.delete_parallel_dfs_derived()
    perf.finished_step(log_message=f"Deleted all duplicate parallel [:DF] edges done")

    graph.merge_duplicate_df()
    perf.finished_step(log_message=f"Merged duplicate [:DF] edges done")

    # graph.df_class_relations()
    # perf.finished_step(log_message=f"[:DF_C] edges done")


def main() -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    if use_preloaded_files:
        print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
    else:
        print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

    # performance class to measure performance
    perf = Performance(perf_path, number_of_steps=number_of_steps)
    graph = create_graph_instance(perf)

    if step_clear_db:
        clear_graph(graph=graph, perf=perf)

    if step_populate_graph:
        populate_graph(graph=graph, perf=perf)

    perf.finish()
    perf.save()

    graph.print_statistics()

    db_connection.close_connection()


if __name__ == "__main__":
    main()
