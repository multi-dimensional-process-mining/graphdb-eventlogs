import json
import os
import random
import warnings
from typing import List, Any, Optional, Self, TypeVar, Generic

import pandas as pd
from pandas import DataFrame

from dataclasses import dataclass

from csv_to_eventgraph_neo4j.auxiliary_functions import replace_undefined_value, create_list


@dataclass
class Class:
    label: str
    required_attributes: List[str]
    ids: List[str]
    df: bool
    include_label_in_cdf: bool
    df_entity_labels: List[str]
    threshold: int
    relative_threshold: int

    @classmethod
    def from_dict(cls, obj: Any) -> Optional['Class']:
        if obj is None:
            return None
        _label = obj.get("label")
        _required_attributes = obj.get("required_attributes")
        _ids = obj.get("ids")
        _df = replace_undefined_value(obj.get("df"), False)
        _include_label_in_cdf = _df or replace_undefined_value(obj.get("include_label_in_cdf"), True)
        _df_entity_labels = obj.get("entity_labels")
        _threshold = replace_undefined_value(obj.get("threshold"), 0)
        _relative_threshold = replace_undefined_value(obj.get("relative_threshold"), 0)
        return cls(_label, _required_attributes, _ids, _df, _include_label_in_cdf, _df_entity_labels, _threshold,
                     _relative_threshold)


@dataclass
class Condition:
    name: str
    values: List[Any]

    @classmethod
    def from_dict(cls, obj: Any) -> Optional['Condition']:
        if obj is None:
            return None
        _name = obj.get("name")
        _values = replace_undefined_value(obj.get("values"), ["IS NOT NULL", '<> "nan"', '<> "None"'])
        if _values != ["IS NOT NULL", '<> "nan"', '<> "None"']:
            _values = [f'''= "{value}"''' for value in _values]
        return cls(_name, _values)


# T_entity_list = TypeVar("T_entity_list", bound=List["Entity"])
# T_entity = TypeVar("T_entity", bound=List["Entity"])
class Entity:

    def __init__(self, include: bool, type: str, labels: List[str], primary_keys: List[str], conditions: List[Condition],
                 corr: bool, df: bool, include_label_in_df: bool, merge_duplicate_df: bool, delete_parallel_df: bool):
        self.include = include
        self.type = type
        self.labels = self.determine_labels(labels)
        self.primary_keys = primary_keys
        self.conditions = conditions
        self.corr = corr
        self.df = df
        self.include_label_in_df = include_label_in_df
        self.merge_duplicate_df = merge_duplicate_df
        self.delete_parallel_df = delete_parallel_df

    def determine_labels(self, labels: List[str]) -> List[str]:
        if "Entity" in labels:
            labels.remove("Entity")

        if self.type not in labels:
            labels.insert(0, self.type)

        return labels

    def get_id_attribute_name(self):
        return self.primary_keys
        for event_attribute in self.event_attributes:
            if event_attribute.is_id:
                return event_attribute._name
        return None

    def get_properties(self):
        properties = {}
        for event_attribute in self.conditions:
            properties[event_attribute.name] = event_attribute.values

        return properties



    @classmethod
    def from_dict(cls, obj: Any) -> Optional['Entity']:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        _type = obj.get("type")
        _labels = replace_undefined_value(obj.get("labels"), [])
        _primary_keys = obj.get("primary_keys")
        _conditions = create_list(Condition, obj.get("conditions"))
        _corr = _include or replace_undefined_value(obj.get("corr"), False)
        _df = _corr or replace_undefined_value(obj.get("df"), False)
        _include_label_in_df = _df or replace_undefined_value(obj.get("include_label_in_df"), False)
        _merge_duplicate_df = _include or replace_undefined_value(obj.get("merge_duplicate_df"), False)
        _delete_parallel_df = _include or replace_undefined_value(obj.get("delete_parallel_df"), False)
        return cls(_include, _type, _labels, _primary_keys, _conditions, _df, _include_label_in_df, _corr,
                      _merge_duplicate_df, _delete_parallel_df)


@dataclass
class Log:
    include: bool
    has: bool

    @classmethod
    def from_dict(cls, obj: Any) -> 'Log':
        if obj is None:
            return Log(True, True)
        _include = replace_undefined_value(obj.get("include"), True)
        _has = replace_undefined_value(obj.get("has"), True)
        return cls(_include, _has)


@dataclass
class Relation:
    include: bool
    type: str
    from_node_label: str
    to_node_label: str
    event_reference_attribute: str
    reify: bool
    reified_entity: Entity

    @classmethod
    def from_dict(cls, obj: Any) -> Optional['Relation']:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        _type = obj.get("type")
        _from_node_label = obj.get("from_node_label")
        _to_node_label = obj.get("to_node_label")
        _event_reference_attribute = obj.get("event_reference_attribute")
        _reified_entity = Entity.from_dict(obj.get("reified_entity"))
        _reify = replace_undefined_value(obj.get("reify"), _reified_entity is not None)
        return cls(_include, _type, _from_node_label, _to_node_label, _event_reference_attribute, _reify,
                        _reified_entity)





class SemanticHeader:
    def __init__(self, name: str, version: str, entities: List[Entity],
                 relations: List[Relation], classes: List[Class],
                 log: Log):
        self.name = name
        self.version = version

        self.entities = entities
        self.relations = relations
        self.classes = classes
        self.log = log

    def get_entity(self, entity_label) -> Optional['Entity']:
        for entity in self.entities:
            if entity_label == entity.label:
                return entity
        return None

    @classmethod
    def from_dict(cls, obj: Any) -> Optional[Self]:
        if obj is None:
            return None

        _name = obj.get("name")
        _version = obj.get("version")
        _entities = create_list(Entity, obj.get("entities"))
        _relations = create_list(Relation, obj.get("relations"))
        _classes = create_list(Class, obj.get("classes"))
        _log = Log.from_dict(obj.get("log"))
        return cls(_name, _version, _entities, _relations,
                              _classes, _log)

    @classmethod
    def create_semantic_header(cls, dataset_name:str):
        with open(f'../json_files/{dataset_name}.json') as f:
            json_semantic_header = json.load(f)

        semantic_header = cls.from_dict(json_semantic_header)
        return semantic_header

