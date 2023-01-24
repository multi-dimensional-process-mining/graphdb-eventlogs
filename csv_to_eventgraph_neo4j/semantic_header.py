import json
import os
import random
import warnings
from typing import List, Any, Dict

import pandas as pd
from pandas import DataFrame

from dataclasses import dataclass

def replace_undefined_value(item, value):
    return item if item is not None else value

@dataclass
class DatetimeObject:
    format: str
    timezone_offset: str
    convert_to: str

    @staticmethod
    def from_dict(obj: Any) -> 'DatetimeObject':
        if obj is None:
            return None
        _format = obj.get("format")
        _timezone_offset = replace_undefined_value(obj.get("timezone_offset"), "")
        _convert_to = str(obj.get("convert_to"))
        return DatetimeObject(_format, _timezone_offset, _convert_to)


@dataclass
class Column:
    name: str
    dtype: str

    @staticmethod
    def from_dict(obj: Any) -> 'Column':
        if obj is None:
            return None
        _name = obj.get("name")
        _dtype = obj.get("dtype")
        return Column(_name, _dtype)


@dataclass
class NaRepColumn:
    name: str
    dtype: str

    @staticmethod
    def from_dict(obj: Any) -> 'NaRepColumn':
        if obj is None:
            return None
        _name = obj.get("name")
        _dtype = obj.get("dtype")
        return NaRepColumn(_name, _dtype)


@dataclass
class Attribute:
    name: str
    columns: List[Column]
    is_datetime: bool
    is_compound: bool
    mandatory: bool
    datetime_object: DatetimeObject
    na_rep_columns: List[NaRepColumn]
    separator: str

    @staticmethod
    def from_dict(obj: Any) -> 'Attribute':
        if obj is None:
            return None
        _name = obj.get("name")
        _columns = [Column.from_dict(y) for y in obj.get("columns")]
        _is_datetime = bool(obj.get("is_datetime"))
        _is_compound = bool(obj.get("is_compound"))
        _mandatory = bool(obj.get("mandatory"))
        _datetime_object = DatetimeObject.from_dict(obj.get("datetime_object"))
        _na_rep_columns = [NaRepColumn.from_dict(y) for y in obj.get("na_rep_columns")] if obj.get(
            "na_rep_columns") is not None else None
        _separator = obj.get("separator")
        return Attribute(_name, _columns, _is_datetime, _is_compound, _mandatory, _datetime_object, _na_rep_columns,
                         _separator)


@dataclass
class Class:
    label: str
    required_attributes: List[str]
    ids: List[str]
    df: bool
    entity_type: str
    threshold: int
    relative_threshold: int

    @staticmethod
    def from_dict(obj: Any) -> 'Class':
        if obj is None:
            return None
        _label = obj.get("label")
        _required_attributes = obj.get("required_attributes")
        _ids = obj.get("ids")
        _df = replace_undefined_value(obj.get("DF"), False)
        _entity_type = obj.get("entity_type")
        _threshold = replace_undefined_value(obj.get("threshold"), 0)
        _relative_threshold = replace_undefined_value(obj.get("relative_threshold"), 0)
        return Class(_label, _required_attributes, _ids, _df, _entity_type, _threshold, _relative_threshold)


@dataclass
class EventAttribute:
    name: str
    values: List[Any]
    is_id: bool

    @staticmethod
    def from_dict(obj: Any) -> 'EventAttribute':
        if obj is None:
            return None
        _name = obj.get("name")
        _values = replace_undefined_value(obj.get("values"), ["IS NOT NULL", '<> "nan"', '<> "None"'])
        _is_id = replace_undefined_value(obj.get("is_id"), False)
        return EventAttribute(_name, _values, _is_id)


class Entity:

    def __init__(self, label: str, event_attributes: List[EventAttribute], df: bool, corr: bool,
                 merge_duplicate_df: bool):
        self.label = label
        self.event_attributes = event_attributes
        self.df = df
        self.corr = corr
        self.merge_duplicate_df = merge_duplicate_df

    def get_id_attribute_name(self):
        for event_attribute in self.event_attributes:
            if event_attribute.is_id:
                return event_attribute.name
        return None

    def get_properties(self):
        properties = {}
        for event_attribute in self.event_attributes:
            properties[event_attribute.name] = event_attribute.values

        return properties

    def get_label(self):
        label = self.label if self.label is not None else ""
        return label

    @staticmethod
    def from_dict(obj: Any) -> 'Entity':
        if obj is None:
            return None
        _label = obj.get("label")
        _event_attributes = [EventAttribute.from_dict(y) for y in obj.get("event_attributes")]
        _df = replace_undefined_value(obj.get("DF"), False)
        _corr = replace_undefined_value(obj.get("CORR"), False)
        _merge_duplicate_df = replace_undefined_value(obj.get("merge_duplicate_df"), False)
        return Entity(_label, _event_attributes, _df, _corr, _merge_duplicate_df)


@dataclass
class ReifiedEntity:
    label: str
    df: bool
    corr: bool
    delete_parallel_df: bool
    merge_duplicate_df: bool

    @staticmethod
    def from_dict(obj: Any) -> 'ReifiedEntity':
        if obj is None:
            return None

        _label = obj.get("label")
        _df = replace_undefined_value(obj.get("DF"), False)
        _corr = replace_undefined_value(obj.get("CORR"), False)
        _delete_parallel_df = replace_undefined_value(obj.get("delete_parallel_df"), False)
        _merge_duplicate_df = replace_undefined_value(obj.get("merge_duplicate_df"), False)

        return ReifiedEntity(_label, _df, _corr, _delete_parallel_df, _merge_duplicate_df)


@dataclass
class Relation:
    type: str
    from_node_label: str
    to_node_label: str
    event_reference_attribute: str
    reify: bool
    reified_entity: ReifiedEntity

    @staticmethod
    def from_dict(obj: Any) -> 'Relation':
        if obj is None:
            return None
        _type = obj.get("type")
        _from_node_label = obj.get("from_node_label")
        _to_node_label = obj.get("to_node_label")
        _event_reference_attribute = obj.get("event_reference_attribute")
        _reify = replace_undefined_value(obj.get("reify"), False)
        _reified_entity = ReifiedEntity.from_dict(obj.get("reified_entity"))
        return Relation(_type, _from_node_label, _to_node_label, _event_reference_attribute, _reify, _reified_entity)


@dataclass
class Sample:
    file_name: str
    sample_method: str
    population_column: str
    size: int
    ids: List[Any]

    @staticmethod
    def from_dict(obj: Any) -> 'Sample':
        if obj is None:
            return None
        _file_name = obj.get("file_name")
        _sample_method = obj.get("sample_method")
        _population_column = obj.get("population_column")
        _size = obj.get("size")
        _ids = obj.get("ids")
        return Sample(_file_name, _sample_method, _population_column, _size, _ids)


class EventTable:
    def __init__(self, name: str, file_names: List[str], labels: List[str], true_values: List[str],
                 false_values: List[str], samples: Dict[str, Sample], attributes: List[Attribute],
                 use_sample: bool = False):
        self.name = name
        self.file_names = file_names
        self.labels = labels
        self.true_values = true_values
        self.false_values = false_values
        self.samples = samples
        self.attributes = attributes
        self.use_sample = use_sample
        # self.attributes = {attribute.name: attribute for attribute in self.attributes}

    @staticmethod
    def from_dict(obj: Any, use_sample: bool = False) -> 'EventTable':
        if obj is None:
            return None

        _name = obj.get("name")
        _file_names = obj.get("file_names")
        _labels = obj.get("labels")
        _true_values = obj.get("true_values")
        _false_values = obj.get("false_values")
        _samples = [Sample.from_dict(y) for y in obj.get("samples")]
        _samples = {sample.file_name: sample for sample in _samples}
        _attributes = [Attribute.from_dict(y) for y in obj.get("attributes")]
        return EventTable(_name, _file_names, _labels, _true_values, _false_values, _samples, _attributes, use_sample)

    def get_dtype_dict(self):
        dtypes = {}
        for attribute in self.attributes:
            for column in attribute.columns:
                if column.dtype is not None:
                    if column.name not in dtypes:
                        dtypes[column.name] = column.dtype
                    else:
                        if column.dtype != dtypes[column.name]:
                            warnings.warn(
                                f"Multiple dtypes ({column.dtype} != {dtypes[column.name]}) "
                                f"defined for {column.name}")
        return dtypes

    def get_required_columns(self):
        required_columns = set()
        for attribute in self.attributes:
            # add column names to the required columns
            required_columns.update([x.name for x in attribute.columns])

        return list(required_columns)

    def create_sample(self, file_name, df_log):
        if self.samples is None:
            warnings.warn(f"No sample population has been defined for {self.name}")

        if file_name not in self.samples:
            #TODO make error
            warnings.warn(f"No sample population has been defined for {file_name}")

        sample = self.samples[file_name]
        sample_column = sample.population_column
        if sample.sample_method == "random":
            random_selection = random.sample(df_log[sample_column].unique().tolist(), k=sample.size)
        else:
            random_selection = sample.ids

        df_log = df_log[df_log[sample_column].isin(random_selection)]

        return df_log

    @staticmethod
    def replace_nan_values(df_log, attribute):
        if len(attribute.na_rep_columns) != len(attribute.columns):
            # TODO make error
            warnings.warn(
                f"Na_rep_columns does not have the same size as columns for attribute {attribute.name}")
        else:  # they are the same size
            for column, na_rep_column in zip(attribute.columns, attribute.na_rep_columns):
                column_name = column.name
                na_rep_column_name = na_rep_column.name

                df_log[column_name].fillna(df_log[na_rep_column_name], inplace=True)

        return df_log

    @staticmethod
    def create_compound_attribute(df_log, attribute):
        compound_column_names = [x.name for x in attribute.columns]
        df_log[attribute.name] = df_log[compound_column_names].apply(
            lambda row: attribute.separator.join(row.values.astype(str)), axis=1)
        return df_log

    @staticmethod
    def rename_column(df_log, attribute):
        column_name = attribute.columns[0].name
        if attribute.name != column_name:  # we rename the attribute
            df_log = df_log.rename(columns={column_name: attribute.name})
        return df_log

    def preprocess_according_to_attributes(self, df_log):
        # loop over all attributes and check if they should be created, renamed or imputed
        for attribute in self.attributes:
            if attribute.na_rep_columns is not None:  # impute values in case of missing values
                df_log = EventTable.replace_nan_values(df_log, attribute)
            if attribute.is_compound:  # create attribute by composing
                df_log = EventTable.create_compound_attribute(df_log, attribute)
            else:  # not compound, check for renaming
                df_log = EventTable.rename_column(df_log, attribute)

        return df_log

    def prepare_data_set(self, input_path, file_name):
        dtypes = self.get_dtype_dict()
        required_columns = self.get_required_columns()

        true_values = self.true_values
        false_values = self.false_values

        df_log: DataFrame = pd.read_csv(os.path.realpath(input_path + file_name), keep_default_na=True,
                                        usecols=required_columns, dtype=dtypes, true_values=true_values,
                                        false_values=false_values)

        if self.use_sample:
            df_log = self.create_sample(file_name, df_log)

        df_log = self.preprocess_according_to_attributes(df_log)

        # all columns have been renamed to or constructed with the name attribute,
        # hence only keep those that match with a name attribute
        required_attributes = list(set([attribute.name for attribute in self.attributes]))
        df_log = df_log[required_attributes]

        return df_log

    def get_datetime_formats(self):
        datetime_formats = {}

        for attribute in self.attributes:
            if attribute.is_datetime:
                datetime_formats[attribute.name] = attribute.datetime_object

        return datetime_formats


class SemanticHeader:
    def __init__(self, name: str, version: str, sample_seed: int,
                 event_tables: List[EventTable], entities: List[Entity],
                 relations: List[Relation], classes: List[Class],
                 use_sample: bool):
        self.name = name
        self.version = version
        self.sample_seed = sample_seed
        random.seed(self.sample_seed)

        self.event_tables = event_tables
        # self.event_tables = {event_table.name: event_table for event_table in self.event_tables}

        self.entities = entities
        # self.entities = {entity.label: entity for entity in self.entities}

        self.relations = relations

        self.classes = classes
        # self.classes = {_class.label: _class for _class in self.classes}

        self.use_sample = use_sample

    @staticmethod
    def from_dict(obj: Any, use_sample: bool = False) -> 'SemanticHeader':
        if obj is None:
            return None

        _name = obj.get("name")
        _version = obj.get("version")
        _sample_seed = replace_undefined_value(obj.get("sample_seed"), 1)
        _event_tables = [EventTable.from_dict(y, use_sample) for y in obj.get("event_tables")]
        _entities = [Entity.from_dict(y) for y in obj.get("entities")]
        _relations = [Relation.from_dict(y) for y in obj.get("relations")]
        _classes = [Class.from_dict(y) for y in obj.get("classes")]
        return SemanticHeader(_name, _version, _sample_seed, _event_tables, _entities, _relations,
                              _classes, use_sample=use_sample)
