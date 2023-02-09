import math

from tqdm import tqdm

from csv_to_eventgraph_neo4j.db_connection import DatabaseConnection
from csv_to_eventgraph_neo4j.event_table import EventTables
from csv_to_eventgraph_neo4j.performance_handling import Performance
from csv_to_eventgraph_neo4j.query_library import CypherQueryLibrary
import pandas as pd


class EventImporter:
    def __init__(self, db_connection: DatabaseConnection, event_tables: EventTables, batch_size: int, use_sample: bool = False,
                 perf: Performance = None):
        self.connection = db_connection
        self.event_tables = event_tables
        self.batch_size = batch_size
        self.use_sample = use_sample
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def import_events(self) -> None:
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
                    self.connection.exec_query(CypherQueryLibrary.get_make_timestamp_date_query,
                                               **{"attribute": attribute, "datetime_object": datetime_format})

                self._write_message_to_performance(
                    f"Reformatted timestamps from events from event table {event_table.name}: {file_name}")

                for boolean in (True, False):
                    attribute_values_pairs_filtered = event_table.get_attribute_value_pairs_filtered(exclude=boolean)
                    for name, values in attribute_values_pairs_filtered:
                        self.connection.exec_query(CypherQueryLibrary.get_filter_events_by_property_query,
                                                   **{"prop": name, "values": values, "exclude": boolean})

                self._write_message_to_performance(
                    f"Filtered the events from event table {event_table.name}: {file_name}")

                # finalize the import
                self.connection.exec_query(CypherQueryLibrary.get_finalize_import_events_query)

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
            self.connection.exec_query(CypherQueryLibrary.get_create_events_batch_query,
                                       **{"batch": batch_without_nans, "labels": labels})

            pbar.update(1)
            batch += 1
        pbar.close()
