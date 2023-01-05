from typing import Tuple, Dict, List, Union, Sequence
from pandas import DataFrame

from EventKnowledgeGraph import EventKnowledgeGraph

# several steps of import, each can be switch on/off
from performance_handling import Performance
from colorama import Fore

step_sample = True
use_preloaded_files = False  # if false, read/import files instead

step_clear_db = True  # entire graph shall be cleared before starting a new import
step_populate_graph = True  # populate the graph
step_filter_events = True
step_createLog = True
step_createEntityRelations = True
step_reifyRelations = True
step_createDF = True
step_deleteParallelDF = True
step_createEventClasses = True
step_createDFC = True
step_createHOWnetwork = True

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

graph_location = 'C:\\Users\\avasw\\.Neo4jDesktop\\relate-data\\dbmss\\' \
                 'dbms-a742a5ee-d1bb-45c5-9bb1-afa291d5c34b\\import\\'


def create_graph_instance() -> EventKnowledgeGraph:
    """
    Creates an instance of an EventKnowledgeGraph
    @return: EventKnowledgeGraph
    """
    return EventKnowledgeGraph(batch_size=10000, path=graph_location,
                               option_df_entity_type_in_label=True, verbose=verbose)

def clear_graph(graph: EventKnowledgeGraph) -> None:
    """
    # delete all nodes and relations in the graph to start fresh
    @param graph: CypherGraph
    @return: None
    """

    print("Clearing DB...")
    graph.clear_db()

def populate_graph(file_name:str, graph: EventKnowledgeGraph, perf: Performance):
    """ STEP C1: IMPORT STATIC NODES"""
    graph.create_static_nodes_and_relations()

    """  STEP C3: IMPORT EVENT NODES"""
    # import the events from all sublogs in the graph with the corresponding labels
    graph.create_events(input_path = graph_location + file_name)
    perf.finished_step(activity=f"Imported events from event log", log_message=f"Event nodes for {file_name} done")

    graph.set_constraints()
    perf.finished_step(activity=f"Set constraints", log_message=f"Constraints are set")

    if step_filter_events:
        graph.filter_events_by_property(prop = 'lifecycle', values = ["SUSPEND","RESUME"])
        perf.finished_step(activity=f"Events are filtered on lifecycle", log_message=f"Events are filtered")

    if step_createLog:
        graph.create_log()
        perf.finished_step(activity=f"Log nodes have been created and events nodes are related with [:HAS] relation", log_message=f"Created Log nodes")

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

    if step_createDF:
        pass

    if step_deleteParallelDF:
        pass

    if step_createEventClasses:
        pass

    if step_createDFC:
        pass

    if step_createHOWnetwork:
        pass

    # # wafers are not associated to a change cassette event in the data sources
    # # do this using the event graph
    # # INFERENCE RULE 1
    # """ STEP INF1"""
    # if inference_rules:
    #     print("Correlating events to entities using logic")
    #     graph.correlate_wafer_to_change_cassette_event()
    #     perf.finished_step(activity=f"Correlated wafers to change cassette events",
    #                        log_message=f"Correlated wafers to change cassette events done")
    #
    #
    #
    # """  STEP C7: RELATE ENTITIES"""
    # # create entity relations to connect the wafer and the slot
    # print(f"Creating Entity Relations: Wafer_At_Slot")
    # graph.create_entity_relationships(relation_type="Wafer_At_Slot",
    #                                   entity_name1="Slot",
    #                                   entity_name2=names.wafer,
    #                                   reference_from1to2="Slot")
    # perf.finished_step(activity=f"create_entity_relationships Wafer_At_Slot",
    #                    log_message=f"Relation (:{names.wafer}) - [:Wafer_At_Slot] -> (:Slot)  done")
    #
    # """  STEP C7: RELATE ENTITIES"""
    # # create a relation between the wafers and the lot that go together
    # # a wafer is always part of a lot
    # print(f"Creating Entity Relation: '{names.part_of_relation}'")
    # relation_type_part_of = names.part_of_relation
    # entity_name_1_lot = names.split_lot
    # entity_name_2_wafer = names.wafer
    # ref_from_1_to_2 = names.split_lot
    # graph.create_entity_relationships(relation_type=relation_type_part_of,
    #                                   entity_name1=entity_name_1_lot,
    #                                   entity_name2=entity_name_2_wafer,
    #                                   reference_from1to2=ref_from_1_to_2)
    # perf.finished_step(activity=f"create_entity_relationships {relation_type_part_of}",
    #                    log_message=f"Relation (:{entity_name_2_wafer}) - [:{relation_type_part_of}] -> (:{entity_name_1_lot}) done")
    #
    # """ STEP INF3: RELATE ENTITIES USING DOMAIN KNOWLEDGE"""
    # # INFERENCE RULE 2
    # # E_Location events are not related to a wafer in the data sources, but are in reality
    # # Use the event graph to correlate the e_location events to the correct wafer
    # if inference_rules:
    #     print(f"Correlate {names.location_label} events to the correct wafer using the slot")
    #     graph.correlate_events_to_wafer(rel_type="Wafer_At_Slot")
    #     perf.finished_step(activity=f"Correlated  {names.location_label} events",
    #                        log_message=f"Correlated  {names.location_label} events nodes")
    #
    #
    # """  IMPROVEMENT STEP 1"""
    # if change_end_events:
    #     # for some events, we know that they should happen before a certain event
    #     # if this is not the case, then we know that the timestamp is wrong
    #     # we set the timestamp to 1 millisecond smaller than the event before which it should have happened
    #     print("\t - Repair the timestamps")
    #     graph.create_directly_follows_tracing(names.wafer)
    #     graph.repair_timestamps()
    #     perf.finished_step(activity=f"Timestamps repaired",
    #                        log_message=f"\t Timestamps repaired done")
    #     """ Not in report:  Implementation Detail"""
    #     # since we change the timestamps, it could be that the event is now related to the wrong wafer
    #     # hence we fix that
    #     print(f"\t - Fix relation from event to wafer")
    #     graph.fix_wafers()
    #     # Delete df, is in report
    #     graph.delete_df(names.wafer)
    #     perf.finished_step(activity=f"Correlations wafers fixed",
    #                        log_message=f"\t Correlation wafers fixed done")
    #
    # # wafer_events are events that happened to the wafer at some location
    # # therefore, we know that the events happen when the wafers are located at that equipment
    #
    # """  STEP INF4: Correlate close events to each other (INFERENCE RULE 3)"""
    # # INFERENCE RULE 2
    # graph.set_load_loc_tracing_events()
    # if inference_rules:
    #     for (wafer_event, wafer_lt, loc_event, pre_seconds, seconds) in pairings:
    #         # graph.set_location_fdc_events(fdc_activity = wafer_event, trac_location = loc_event)
    #         graph.set_location_fdc_events(wafer_event, loc_event)
    #         graph.correlate_e_wafer_event_to_wafers_based_on_time(fdc_event=wafer_event, wafer_lt=wafer_lt,
    #                                                               tracing_event=loc_event,
    #                                                               pre_seconds=pre_seconds, seconds=seconds)
    #         perf.finished_step(activity=f"Related {wafer_event} to :{names.wafer}",
    #                            log_message=f"\t Related {wafer_event} to :{names.wafer} done")
    #
    # """  STEP C8: CREATE WAFERLOTS"""
    # # Combine lots that have wafers with the same starting ID
    # print(f"Create {names.combined_lot} Nodes")
    # graph.create_combined_lot()
    # perf.finished_step(activity=f"Created {names.combined_lot} Nodes",
    #                    log_message=f"Created {names.combined_lot} Nodes done")
    #
    # graph.merge_lot_nodes()
    # perf.finished_step(activity=f"merge_lot_nodes",
    #                    log_message=f"Merging virtual and actual nodes  done")
    #
    # """ STEP C9: CONVERT AOI EVENTS"""
    # # # Relate Batched AOI events and AOI results to the correct entities
    # # batch aoi events and create AOI_result entities
    # graph.create_batched_event()
    # graph.add_yield()
    # perf.finished_step(activity=f"Batched AOI events",
    #                    log_message=f"Batched AOI events done")
    #
    # """ STEP C10: Create DF"""
    # # create directly follows relations for the cassette, lot and connector
    # print("Creating Directly Follows Edges")
    #
    # entities = [names.wafer, names.split_lot, "Blade"]
    # for entity_type_in_event in entities:
    #     graph.create_directly_follows(entity_type_in_event)
    #     perf.finished_step(activity=f"create_df '{entity_type_in_event}'",
    #                        log_message=f"DF for Entity '{entity_type_in_event}' done")
    #
    # """Continuation IMPROVEMENT STEP 1: Implementation Detail"""
    # if change_end_events:
    #     # in some cases, the new wafer_events result in that the events that were changed still have an incorrect timestamp
    #     # we look specifically at these changed events and see whether they should be swapped with an event next to it
    #     print("\t - Fix timestamps of changed events that are incorrect due to the new wafer events")
    #     graph.fix_changed_events_with_new_wafer_events()
    #     perf.finished_step(activity=f"Fix timestamps of changed events",
    #                        log_message=f"\t Fix timestamps of changed events done")
    #
    # """ STEP C11: Merge and delete the nodes"""
    # # register wafer events are an artifact from the data and do not actually happen
    # # remove them from the graph
    # graph.delete_nodes(label=names.wafer_label, properties={names.activity: "RegisterWafer"})
    # graph.update_df(names.wafer)
    #
    # perf.finished_step(activity=f"Delete RegisterWafer nodes",
    #                    log_message=f"\t Delete RegisterWafer nodes")
    # """Following two steps are not in report, are implementation detail"""
    # if change_end_events:
    #     # Because sometimes some seconds are added/subtracted to the tracing events, it could be that fdc event has already
    #     # ended before the tracing event has started. The time difference between the fdc event end and tracing start is minimal
    #
    #     # Hence instead of A (fdc start) -> A' (tracing start) -> B (fdc end)        -> B' (tracing end),
    #     #          we have A (fdc start) -> B (fdc end)        -> A' (tracing start) -> B' (tracind end).
    #     # these events won't be merged, hence we redirect them such that A -> A' -> B -> B'
    #     graph.redirect_df_loading_cutting()
    #     graph.redirect_df_wafer_to_clean()
    #     perf.finished_step(activity=f"Redirected DF relations",
    #                        log_message=f"Redirected DF relations")
    #
    # if merge_nodes:
    #     for (wafer_event, loc_event) in limited_pairings:
    #         graph.merge_nodes(wafer_event, loc_event)
    #     for (wafer_event, loc_event) in limited_pairings:
    #         graph.rename_nodes(wafer_event, loc_event)
    #
    #     graph.update_df(names.wafer)
    #
    #     perf.finished_step(activity=f"Merged and Renamed Event nodes",
    #                        log_message=f"Merged and Renamed Event nodes")
    #
    #
    # """  STEP INF2: Correlate events to entities --> Inference rule #2 simplified"""
    # # INFERENCE RULE 2
    # if inference_rules:
    #     print("Correlating blades to events + properties")
    #     graph.correlate_cutting_events_to_blade()
    #     perf.finished_step(activity=f"Correlated blades to cutting events",
    #                        log_message=f"Correlated blades to cutting events")
    #
    # """STEP C12: ASSOCIATE EVENTS TO LOCATIONS"""
    # graph.add_locations()
    # perf.finished_step(activity=f"Associate event nodes to locations",
    #                    log_message=f"Associate event nodes to locations")
    #
    # """ STEP C13: CREATE DUMMY WAFERS --> SPLIT BARCODE READING"""
    # graph.split_barcode_reading()
    # graph.delete_redundant_nodes()
    # graph.update_df(names.wafer)
    # perf.finished_step(activity=f"Create Dummy Wafers",
    #                    log_message=f"Create Dummy Wafers")
    #
    # """ STEP C14: Create DF_E between wafers"""
    # # add DF_E for Wafers
    # print(f"\t - Creating DF_E for {names.wafer}")
    # graph.create_e_df_between_wafers()
    # perf.finished_step(activity=f"Create DF_E for {names.wafer}",
    #                    log_message=f"\t Created DF_E for {names.wafer} done")
    #
    # """ IMPROVEMENT STEP 2: IMPROVE STATE"""
    # if improve_state:
    #     graph.repair_timestamps_cassette()
    #     graph.repair_wafers_wrong_cassette()
    #     graph.update_df(names.wafer)
    #
    # """ STEP C15: Correlate Event nodes to EState nodes"""
    # graph.relation_state_events()
    #
    #
    # if water_analysis:
    #     graph.delete_df("Water")
    #     entity_map = {"WaterZ1": ["E_State", {names.activity: "WaterStateChange",
    #                                           "BladeType": "Z1"}],
    #                   "WaterZ2": ["E_State", {names.activity: "WaterStateChange",
    #                                           "BladeType": "Z2"}],
    #                   "WaterSummaryZ1": ["E_State", {names.activity: "WaterStateChange",
    #                                           "BladeType": "Z1",
    #                                            "summary": "true"}],
    #                   "WaterSummaryZ2": ["E_State", {names.activity: "WaterStateChange",
    #                                           "BladeType": "Z2",
    #                                            "summary": "true"}]
    #                   }
    #
    #     for entity, (label, properties) in entity_map.items():
    #         entity = entity.replace("Z1", "").replace("Z2", "")
    #         graph.create_df_wo_entities(label, properties, entity)
    #
    #     graph.determine_water_summary()
    #
    #     graph.delete_df("Water")
    #
    #     entity_map = {"WaterZ1": ["E_State", {names.activity: "WaterStateChange",
    #                                           "BladeType": "Z1",
    #                                           "summary": "NULL"}],
    #                   "WaterZ2": ["E_State", {names.activity: "WaterStateChange",
    #                                           "BladeType": "Z2",
    #                                           "summary": "NULL"}]
    #                   }
    #
    #     for entity, (label, properties) in entity_map.items():
    #         entity = entity.replace("Z1", "").replace("Z2", "")
    #         graph.create_df_wo_entities(label, properties, entity)
    #
    # graph.delete_df(relation_type=names.split_lot)
    # graph.delete_properties(label="Event", property="Slot")
    # graph.delete_properties(label="Event", property="PairID")
    # graph.delete_properties(label="Event", property="Wafer")
    # graph.delete_properties(label="Event", property=names.split_lot)
    # # graph.delete_nodes("Water")
    # # graph.delete_nodes("State")
    #
    # # Delete redundant nodes and the DF edges
    # print(f"Deleting PairID Nodes")
    # print(f"Delete PairID nodes")
    # graph.delete_nodes("PairID", None)
    # perf.finished_step(activity=f"Deleted PairID Nodes",
    #                    log_message=f"Deleted PairID Nodes done")
    # if delete_connector:
    #     print(f"Delete Connector nodes and its DF edges")
    #     graph.delete_nodes("Connector", None)
    #     graph.delete_df("Connector")
    # perf.finished_step(activity=f"Deleted Connector Nodes and DF_Connector",
    #                    log_message=f"Deleted Connector Nodes and DF_Connector done")
    #
    # if delete_non_matched_events:
    #     graph.delete_non_matched_events()
    #
    # # STEP ADD KPI
    # graph.calculate_UPH()
    # graph.create_df_wo_entities("KPI", None, "KPI")

def main() -> None:
    """
    Main function, read all the logs, clear and create the graph, perform checks
    @return: None
    """
    if use_preloaded_files:
        print(Fore.RED + '💾 Preloaded files are used!' + Fore.RESET)
    else:
        print(Fore.RED + '📝 Importing and creating files' + Fore.RESET)

    if step_sample:
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
        clear_graph(graph=graph)

    if step_populate_graph:
        populate_graph(file_name = file_name, graph=graph, perf=perf)

    # if step_add_classes:
    #     add_classes(graph=graph, perf=perf)

    if step_populate_graph or step_add_classes:
        # all actions are completed, thus the performance can be finished and saved
        perf.finish()
        perf.save()

    graph.close_connection()


if __name__ == "__main__":
    main()