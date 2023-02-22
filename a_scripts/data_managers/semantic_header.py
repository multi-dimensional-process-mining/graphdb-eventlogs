import json
from abc import ABC
from typing import List, Any, Optional, Self, Union

from dataclasses import dataclass

from a_scripts.additional_functions.auxiliary_functions import replace_undefined_value, create_list


@dataclass
class Class(ABC):
    label: str
    class_identifiers: List[str]
    ids: List[str]

    @classmethod
    def from_dict(cls, obj: Any) -> Optional[Self]:
        if obj is None:
            return None
        _label = obj.get("label")
        _class_identifiers = obj.get("class_identifiers")
        _ids = obj.get("ids")
        return cls(_label, _class_identifiers, _ids)


@dataclass
class Condition:  # TODO convert to abc, replace undefined values
    attribute: str
    values: List[Any]

    @classmethod
    def from_dict(cls, obj: Any, not_exist_properties=None) -> Optional[Self]:
        if obj is None:
            return None

        if not_exist_properties is None:
            not_exist_properties = ['<> null']
        _attribute = obj.get("attribute")
        _include_values = replace_undefined_value(obj.get("values"), not_exist_properties)
        return cls(_attribute, _include_values)


@dataclass
class Relation(ABC):
    include: bool
    type: str
    from_node_label: str
    to_node_label: str
    primary_key: str
    foreign_key: str

    @classmethod
    def from_dict(cls, obj: Any) -> Optional[Self]:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _type = obj.get("type")
        _from_node_label = obj.get("from_node_label")
        _to_node_label = obj.get("to_node_label")
        _primary_key = replace_undefined_value(obj.get("primary_key"), "ID")
        _foreign_key = obj.get("foreign_key")
        return cls(_include, _type, _from_node_label, _to_node_label, _primary_key, _foreign_key)


@dataclass
class EntityConstructorByNode(ABC):
    node_label: str
    conditions: List[Condition]

    @classmethod
    def from_dict(cls, obj: Any, condition_class_name: Condition = Condition) -> Optional[Self]:
        if obj is None:
            return None

        _node_label = obj.get("node_label")
        _conditions = create_list(condition_class_name, obj.get("conditions"))

        return cls(node_label=_node_label, conditions=_conditions)


@dataclass
class EntityConstructorByRelation(ABC):
    relation_type: str
    conditions: List[Condition]

    @classmethod
    def from_dict(cls, obj: Any, condition_class_name: Condition = Condition) -> Optional[Self]:
        if obj is None:
            return None

        _relation_type = obj.get("relation_type")
        _conditions = create_list(condition_class_name, obj.get("conditions"))

        return cls(relation_type=_relation_type, conditions=_conditions)


@dataclass
class EntityConstructorByQuery(ABC):
    query: str

    @classmethod
    def from_dict(cls, obj: Any) -> Optional[Self]:
        if obj is None:
            return None

        _query = obj.get("query")

        return cls(query=_query)


@dataclass
class Entity(ABC):
    include: bool
    constructed_by: Union[EntityConstructorByNode, EntityConstructorByRelation, EntityConstructorByQuery]
    constructor_type: str
    type: str
    labels: List[str]
    primary_keys: List[str]
    entity_attributes: List[str]
    entity_attributes_wo_primary_keys: List[str]
    corr: bool
    df: bool
    include_label_in_df: bool
    merge_duplicate_df: bool
    delete_parallel_df: bool

    def get_primary_keys(self):
        return self.primary_keys

    @staticmethod
    def determine_labels(labels: List[str], _type: str) -> List[str]:
        if "Entity" in labels:
            labels.remove("Entity")

        if _type not in labels:
            labels.insert(0, _type)

        return labels

    def get_properties(self):
        properties = {}
        for condition in self.constructed_by.conditions:
            properties[condition.attribute] = condition.values

        return properties

    @classmethod
    def from_dict(cls, obj: Any, condition_class_name: Condition = Condition,
                  relation_class_name: Relation = Relation) -> Optional[Self]:

        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None

        _constructed_by = EntityConstructorByNode.from_dict(obj.get("constructed_by_node"))
        if _constructed_by is None:
            _constructed_by = EntityConstructorByRelation.from_dict(obj.get("constructed_by_relation"))
        if _constructed_by is None:
            _constructed_by = EntityConstructorByQuery.from_dict(obj.get("constructed_by_query"))

        _constructor_type = _constructed_by.__class__.__name__
        _type = obj.get("type")
        _labels = replace_undefined_value(obj.get("labels"), [])
        _labels = Entity.determine_labels(_labels, _type)
        _primary_keys = obj.get("primary_keys")
        _entity_attributes = obj.get("entity_attributes")
        _entity_attributes_wo_primary_keys = [attr for attr in _entity_attributes if attr not in _primary_keys]

        _corr = _include and replace_undefined_value(obj.get("corr"), False)
        _df = _corr and replace_undefined_value(obj.get("df"), False)
        _include_label_in_df = _df and replace_undefined_value(obj.get("include_label_in_df"), False)
        _merge_duplicate_df = _df and replace_undefined_value(obj.get("merge_duplicate_df"), False)

        _delete_parallel_df = _df and obj.get("delete_parallel_df")

        return cls(include=_include, constructed_by=_constructed_by, constructor_type=_constructor_type,
                   type=_type, labels=_labels, primary_keys=_primary_keys,
                   entity_attributes=_entity_attributes,
                   entity_attributes_wo_primary_keys=_entity_attributes_wo_primary_keys,
                   corr=_corr, df=_df, include_label_in_df=_include_label_in_df, merge_duplicate_df=_merge_duplicate_df,
                   delete_parallel_df=_delete_parallel_df)


@dataclass
class Log(ABC):
    include: bool
    has: bool

    @classmethod
    def from_dict(cls, obj: Any) -> Self:
        if obj is None:
            return Log(True, True)
        _include = replace_undefined_value(obj.get("include"), True)
        if not _include:
            return None
        _has = replace_undefined_value(obj.get("has"), True)
        return cls(_include, _has)


class SemanticHeader(ABC):
    def __init__(self, name: str, version: str,
                 entities_derived_from_nodes: List[Entity], entities_derived_from_relations: List[Entity],
                 entities_derived_from_query: List[Entity], relations: List[Relation], classes: List[Class], log: Log):
        self.name = name
        self.version = version

        self.entities_derived_from_nodes = entities_derived_from_nodes
        self.entities_derived_from_relations = entities_derived_from_relations
        self.entities_derived_from_query = entities_derived_from_query
        self.relations = relations
        self.classes = classes
        self.log = log

    def get_entity(self, entity_type) -> Optional[Entity]:
        for entity in self.entities_derived_from_nodes + self.entities_derived_from_relations + \
                      self.entities_derived_from_query:
            if entity_type == entity.type:
                return entity
        return None

    @classmethod
    def from_dict(cls, obj: Any, derived_entity_class_name: Entity = Entity,
                  reified_entity_class_name: Entity = Entity,
                  relation_class_name: Relation = Relation,
                  class_class_name: Class = Class, log_class_name: Log = Log) -> Optional[Self]:
        if obj is None:
            return None
        _name = obj.get("name")
        _version = obj.get("version")
        _entities = create_list(derived_entity_class_name, obj.get("entities"))
        _entities_derived_from_nodes = [entity for entity in _entities if
                                        entity.constructor_type == "EntityConstructorByNode"]
        _entities_derived_from_relations = [entity for entity in _entities if
                                            entity.constructor_type == "EntityConstructorByRelation"]
        _entities_derived_from_query = [entity for entity in _entities if
                                        entity.constructor_type == "EntityConstructorByQuery"]
        _relations = create_list(relation_class_name, obj.get("relations"))
        _classes = create_list(class_class_name, obj.get("classes"))
        _log = log_class_name.from_dict(obj.get("log"))
        return cls(_name, _version, _entities_derived_from_nodes, _entities_derived_from_relations,
                   _entities_derived_from_query, _relations,
                   _classes, _log)

    @classmethod
    def create_semantic_header(cls, dataset_name: str, **kwargs):
        with open(f'../json_files/{dataset_name}.json') as f:
            json_semantic_header = json.load(f)

        semantic_header = cls.from_dict(json_semantic_header, **kwargs)
        return semantic_header
