from typing import List, Set, Optional, Dict

from a_scripts.database_managers.db_connection import DatabaseConnection
from a_scripts.additional_functions.performance_handling import Performance
from a_scripts.database_managers.query_library import CypherQueryLibrary as cql


class EKGManagement:
    def __init__(self, db_connection: DatabaseConnection, db_name, perf: Performance):
        self.connection = db_connection
        self.db_name = db_name
        self.perf = perf

    def _write_message_to_performance(self, message: str):
        if self.perf is not None:
            self.perf.finished_step(activity=message)

    def clear_db(self):
        self.connection.exec_query(cql.get_clear_db_query, **{"db_name": self.db_name})
        self._write_message_to_performance("DB is cleared")

    def set_constraints(self):
        # for implementation only (not required by schema or patterns)
        self.connection.exec_query(cql.get_constraint_unique_event_id_query)
        self._write_message_to_performance("Constraint on unique event IDs is set")

        # required by core pattern
        self.connection.exec_query(cql.get_constraint_unique_entity_uid_query)
        self._write_message_to_performance("Constraint on unique entity uIDs is set")

        self.connection.exec_query(cql.get_constraint_unique_log_id_query)
        self._write_message_to_performance("Constraint on unique log IDs is set")

    def get_all_rel_types(self) -> List[str]:
        """
        Find all possible rel types
        @return:
        """

        # execute the query and store the result
        result = self.connection.exec_query(cql.get_all_rel_types_query)
        # in case there are no rel types, the result is None
        # return in this case an emtpy list
        if result is None:
            return []
        # store the results in a list
        result = [record["rel_type"] for record in result]
        return result

    def get_all_node_labels(self) -> Set[str]:
        """
        Find all possible node labels
        @return: Set of strings
        """

        # execute the query and store the result
        result = self.connection.exec_query(cql.get_all_node_labels)
        # in case there are no labels, return an empty set
        if result is None:
            return set([])
        # some nodes have multiple labels, which are returned as a list of labels
        # therefore we need to flatten the result and take the set
        result = set([record for sublist in result for record in sublist["label"]])
        return result

    def get_statistics(self) -> List[Dict[str, any]]:
        def make_empty_list_if_none(_list: Optional[List[Dict[str, str]]]):
            if _list is not None:
                return _list
            else:
                return []

        node_count = self.connection.exec_query(cql.get_node_count_query)
        edge_count = self.connection.exec_query(cql.get_edge_count_query)
        agg_edge_count = self.connection.exec_query(cql.get_aggregated_edge_count_query)
        result = \
            make_empty_list_if_none(node_count) + \
            make_empty_list_if_none(edge_count) + \
            make_empty_list_if_none(agg_edge_count)
        return result
