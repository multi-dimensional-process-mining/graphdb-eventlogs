from typing import Optional, List, Dict, Set

from csv_to_eventgraph_neo4j.db_connection import DatabaseConnection
from csv_to_eventgraph_neo4j.ekg_builder_semantic_header import EKGUsingSemanticHeaderBuilder
from csv_to_eventgraph_neo4j.ekg_management import EKGManagement
from csv_to_eventgraph_neo4j.datastructures import ImportedDataStructures
from csv_to_eventgraph_neo4j.data_importer import Importer
from csv_to_eventgraph_neo4j.inference_engine import InferenceEngine
from csv_to_eventgraph_neo4j.performance_handling import Performance
from csv_to_eventgraph_neo4j.semantic_header_lpg import SemanticHeaderLPG

from tabulate import tabulate


class EventKnowledgeGraph:
    def __init__(self, db_connection: DatabaseConnection, db_name: str, batch_size: int,
                 event_tables: ImportedDataStructures, use_sample: bool = False,
                 semantic_header: SemanticHeaderLPG = None,
                 perf: Performance = None):
        self.ekg_management = EKGManagement(db_connection=db_connection, db_name=db_name, perf=perf)
        self.data_importer = Importer(db_connection, data_structures=event_tables, batch_size=batch_size,
                                      use_sample=use_sample, perf=perf)
        self.ekg_builder = EKGUsingSemanticHeaderBuilder(db_connection=db_connection, semantic_header=semantic_header,
                                                         batch_size=batch_size, perf=perf)
        self.inference_engine = InferenceEngine(db_connection=db_connection, perf=perf)
        # ensure to allocate enough memory to your database: dbms.memory.heap.max_size=5G advised

    # region EKG management
    """Define all queries and return their results (if required)"""

    def clear_db(self):
        self.ekg_management.clear_db()

    def set_constraints(self):
        self.ekg_management.set_constraints()

    def get_all_rel_types(self) -> List[str]:
        """
        Find all possible rel types
        @return:
        """
        return self.ekg_management.get_all_rel_types()

    def get_all_node_labels(self) -> Set[str]:
        """
        Find all possible node labels
        @return: Set of strings
        """
        return self.ekg_management.get_all_node_labels()

    def print_statistics(self):
        print(tabulate(self.ekg_management.get_statistics()))

    # endregion

    # region import events
    def import_data(self):
        self.data_importer.import_data()

    # endregion

    # region EKG builder using semantic header

    def create_log(self):
        self.ekg_builder.create_log()

    def create_entities(self) -> None:
        self.ekg_builder.create_entities()

    def correlate_events_to_entities(self) -> None:
        self.ekg_builder.correlate_events_to_entities()

    def create_entity_relations(self) -> None:
        self.ekg_builder.create_entity_relations()

    def reify_entity_relations(self) -> None:
        self.ekg_builder.reify_entity_relations()

    def correlate_events_to_reification(self) -> None:
        self.ekg_builder.correlate_events_to_reification()

    def create_df_edges(self) -> None:
        self.ekg_builder.create_df_edges()

    def merge_duplicate_df(self):
        self.ekg_builder.merge_duplicate_df()

    def delete_parallel_dfs_derived(self):
        self.ekg_builder.delete_parallel_dfs_derived()

    def create_classes(self):
        self.ekg_builder.create_classes()

    def add_attributes_to_classifier(self, relation, label, properties, copy_as=None):
        self.ekg_builder.add_attributes_to_classifier(relation, label, properties, copy_as)

    def create_static_nodes_and_relations(self):
        self.ekg_builder.create_static_nodes_and_relations()

    def infer_items_to_load_events(self, entity, is_load=True):
        self.inference_engine.infer_items_to_load_events(entity, is_load)
        self.add_entity_to_event(entity=entity)

    def match_entity_with_batch_position(self, entity):
        self.inference_engine.match_entity_with_batch_position(entity)
        self.add_entity_to_event(entity=entity)

    def match_event_with_batch_position(self, entity):
        self.inference_engine.match_event_with_batch_position(entity)
        self.add_entity_to_event(entity=entity)

    def infer_items_to_events_with_batch_position(self, entity):
        self.inference_engine.infer_items_to_events_with_batch_position(entity)
        self.add_entity_to_event(entity=entity)

    def infer_items_to_administrative_events_using_location(self, entity):
        self.inference_engine.infer_items_to_administrative_events_using_location(entity)
        self.add_entity_to_event(entity=entity)

    def add_entity_to_event(self, entity):
        self.inference_engine.add_entity_to_event(entity)

    # endregion
