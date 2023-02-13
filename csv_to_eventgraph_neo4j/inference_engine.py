from csv_to_eventgraph_neo4j.query_library import CypherQueryLibrary as cql


class InferenceEngine:
    def __init__(self, db_connection, perf):
        self.connection = db_connection
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def infer_batch_items(self, entity, batch_location):
        self.connection.exec_query(cql.infer_batch_items, **{"entity": entity, "batch_location": batch_location})
        self._write_message_to_performance("Batch items are inferred")

    def match_entity_with_batch_position(self, entity):
        self.connection.exec_query(cql.match_entity_with_batch_position_and_preceding_events, **{"entity": entity})
        self._write_message_to_performance("Entities are matched with batch position")

    def infer_items_to_events_with_batch_position(self, entity, batch_location):
        self.connection.exec_query(cql.infer_items_to_events_with_batch_position, **{"entity": entity, "batch_location": batch_location})

    def infer_items_to_manufacturing_events_at_location(self, entity):
        self.connection.exec_query(cql.infer_items_to_manufacturing_events_at_location, **{"entity": entity})

    def add_entity_to_event(self, entity):
        self.connection.exec_query(cql.add_entity_to_event, **{"entity": entity})
