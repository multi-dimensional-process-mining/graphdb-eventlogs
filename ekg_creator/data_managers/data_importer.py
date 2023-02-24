import math

from tqdm import tqdm

from database_managers.db_connection import DatabaseConnection
from data_managers.datastructures import ImportedDataStructures
from utilities.performance_handling import Performance
from database_managers.query_library import CypherQueryLibrary
import pandas as pd


class Importer:
    def __init__(self, db_connection: DatabaseConnection, data_structures: ImportedDataStructures, batch_size: int,
                 use_sample: bool = False,
                 perf: Performance = None):
        self.connection = db_connection
        self.structures = data_structures.structures

        self.batch_size = batch_size
        self.use_sample = use_sample
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def import_data(self) -> None:
        # Cypher does not recognize pd date times, therefore we convert the date times to the correct string format
        for structure in self.structures:
            labels = structure.labels
            file_directory = structure.file_directory
            for file_name in structure.file_names:
                # read and import the events
                df_log = structure.prepare_event_data_sets(file_directory, file_name, self.use_sample)
                df_log["justImported"] = True
                self._import_data_nodes_from_data(labels, df_log, file_name)
                self._write_message_to_performance(f"Imported data from table {structure.name}: {file_name}")

            if structure.is_event_data():
                # once all events are imported, we convert the string timestamp to the timestamp as used in Cypher
                self._reformat_timestamps(structure)
                self._write_message_to_performance(
                    f"Reformatted timestamps from events from event table {structure.name}: {file_name}")
            else:
                self._merge_nodes(structure)
                self._write_message_to_performance(
                    f"Similar nodes from table {structure.name}: {file_name} are merged")

            self._filter_nodes(structure=structure)
            self._write_message_to_performance(
                f"Filtered the nodes from table {structure.name}: {file_name}")

            self._finalize_import(labels=labels)

            self._write_message_to_performance(
                f"Finalized the import from table {structure.name}: {file_name}")


    def _reformat_timestamps(self, structure):
        datetime_formats = structure.get_datetime_formats()
        for attribute, datetime_format in datetime_formats.items():
            self.connection.exec_query(CypherQueryLibrary.get_make_timestamp_date_query,
                                       **{"attribute": attribute, "datetime_object": datetime_format})

    def _merge_nodes(self, structure):
        self.connection.exec_query(CypherQueryLibrary.merge_same_nodes,
                                   **{"data_structure": structure})

    def _filter_nodes(self, structure):
        for boolean in (True, False):
            attribute_values_pairs_filtered = structure.get_attribute_value_pairs_filtered(exclude=boolean)
            for name, values in attribute_values_pairs_filtered:
                self.connection.exec_query(CypherQueryLibrary.get_filter_events_by_property_query,
                                           **{"prop": name, "values": values, "exclude": boolean})

    def _finalize_import(self, labels):
        # finalize the import
        self.connection.exec_query(CypherQueryLibrary.get_finalize_import_events_query,
                                   **{"labels": labels})

    def _import_data_nodes_from_data(self, labels, df_log, file_name):
        # start with batch 0 and increment until everything is imported
        batch = 0
        print("\n")
        pbar = tqdm(total=math.ceil(len(df_log) / self.batch_size), position=0)
        while batch * self.batch_size < len(df_log):
            pbar.set_description(f"Loading data from {file_name} from batch {batch}")

            # import the events in batches, use the records of the log
            batch_without_nans = [{k: v for k, v in m.items()
                                   if (isinstance(v, list) and len(v) > 0) or (not pd.isna(v) and v is not None)}
                                  for m in
                                  df_log[batch * self.batch_size:(batch + 1) * self.batch_size].to_dict(
                                      orient='records')]
            self.connection.exec_query(CypherQueryLibrary.get_create_events_batch_query,
                                       **{"batch": batch_without_nans, "labels": labels})

            pbar.update(1)
            batch += 1
        pbar.close()
