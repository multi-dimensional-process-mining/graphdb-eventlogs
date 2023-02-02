import json
from typing import Optional, Any, Self, List

from csv_to_eventgraph_neo4j.auxiliary_functions import create_list, replace_undefined_value
from csv_to_eventgraph_neo4j.semantic_header import SemanticHeader, Entity, Relation, Class, Log, Condition


class SemanticHeaderLPG(SemanticHeader):
    @classmethod
    def from_dict(cls, obj: Any) -> Optional[Self]:
        if obj is None:
            return None

        _name = obj.get("name")
        _version = obj.get("version")
        _entities = create_list(EntityLPG, obj.get("entities"))
        _relations = create_list(RelationLPG, obj.get("relations"))
        _classes = create_list(ClassLPG, obj.get("classes"))
        _log = LogLPG.from_dict(obj.get("log"))
        return cls(_name, _version, _entities, _relations,
                   _classes, _log)


class EntityLPG(Entity):
    def __init__(self, include: bool, type: str, labels: List[str], primary_keys: List[str],
                 conditions: List[Condition], corr: bool, df: bool, include_label_in_df: bool, merge_duplicate_df: bool,
                 delete_parallel_df: bool):
        super().__init__(include, type, labels, primary_keys, conditions, corr, df, include_label_in_df,
                         merge_duplicate_df, delete_parallel_df)
        self.df_label = self.get_df_label()

    def get_label_string(self):
        return "Entity:" + ":".join(self.labels)

    def get_df_label(self):
        """
        Create the df label based on self.option_DF_entity_type_in_label
        If not in entity type, add it as property to the label
        @param entity:
        @return:
        """
        if self.include_label_in_df:
            return f'DF_{self.type.upper()}'
        else:
            return f'DF'

    def get_composed_primary_id_query(self):
        return "+\"-\"+".join([f"e.{key}" for key in self.primary_keys])

    def get_separate_primary_ids_query(self):
        return ','.join([f"e.{key} as {key}" for key in self.primary_keys])

    def get_separate_primary_keys_properties_query(self):
        return ',\n'.join([f"{key}: {key}" for key in self.primary_keys])

    @staticmethod
    def get_dfc_label(label: str, include_label_in_df: bool):
        """
        Create the df label based on self.option_DF_entity_type_in_label
        If not in entity type, add it as property to the label
        @param label: str, label that should be created in the DF
        @param include_label_in_df:
        @return:
        """
        if include_label_in_df:
            return f'DF_C_{label.upper()}'
        else:
            return f'DF_C'


class RelationLPG(Relation):
    @classmethod  # remove once reified entity is seperate
    def from_dict(cls, obj: Any) -> Optional[Self]:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        _type = obj.get("type")
        _from_node_label = obj.get("from_node_label")
        _to_node_label = obj.get("to_node_label")
        _event_reference_attribute = obj.get("event_reference_attribute")
        _reified_entity = EntityLPG.from_dict(obj.get("reified_entity"))
        _reify = replace_undefined_value(obj.get("reify"), _reified_entity is not None)
        return cls(_include, _type, _from_node_label, _to_node_label, _event_reference_attribute, _reify,
                   _reified_entity)


class ClassLPG(Class):
    pass


class LogLPG(Log):
    pass
