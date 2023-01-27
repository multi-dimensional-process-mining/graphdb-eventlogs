import json

from EventKnowledgeGraph import EventKnowledgeGraph
from csv_to_eventgraph_neo4j.semantic_header import SemanticHeader

# several steps of import, each can be switch on/off
from performance_handling import Performance
from colorama import Fore

import authentication

connection = authentication.connections_map[authentication.Connections.LOCAL]


with open('../json_files/BPIC17.json') as f:
    json_dict = json.load(f)

use_sample = True
semantic_header = SemanticHeader.from_dict(json_dict)
perf_path = '..\\perf\\' + semantic_header.name + "Performance.csv"
number_of_steps = 100

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


def create_graph_instance(perf: Performance) -> EventKnowledgeGraph:
    """
    Creates an instance of an EventKnowledgeGraph
    @return: returns an EventKnowledgeGraph
    """

    return EventKnowledgeGraph(db_name=connection.user, uri=connection.uri, user=connection.user,
                               password=connection.password, batch_size=5000,
                               verbose=verbose, use_sample=use_sample,
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
    graph.create_events()
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

    graph.create_entity_relations()
    perf.finished_step(log_message=f"[:REL] edges done")

    graph.reify_entity_relations_sh()
    perf.finished_step(log_message=f"Reified (:Entity) nodes done")

    graph.correlate_events_to_reification()
    perf.finished_step(log_message=f"[:CORR] edges for Reified (:Entity) nodes done")

    graph.create_df_edges()
    perf.finished_step(log_message=f"[:DF] edges done")

    # TODO: add perf message inside function
    # "Deleted parallel DF between {derived_entity} and {parent_entity}"
    graph.delete_parallel_dfs_derived()
    perf.finished_step(log_message=f"Deleted all duplicate parallel [:DF] edges done")

    # TODO: add perf message inside function
    # "Duplicate DF for Entity '{entity}' are merged"
    graph.merge_duplicate_df()
    perf.finished_step(log_message=f"Merged duplicate [:DF] edges done")

    # TODO: add perf message inside function
    # Created classes for {label}
    graph.create_classes()
    perf.finished_step(log_message=f"(:Class) nodes done")
    graph.df_class_relations()
    perf.finished_step(log_message=f"[:DF_C] edges done")


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

    graph.close_connection()


if __name__ == "__main__":
    main()
