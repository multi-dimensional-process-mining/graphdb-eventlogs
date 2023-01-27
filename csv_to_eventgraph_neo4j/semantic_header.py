import json
import os
import random
import warnings
from typing import List, Any, Dict, Optional

import pandas as pd
from pandas import DataFrame

from dataclasses import dataclass


def replace_undefined_value(item, value):
    return item if item is not None else value


def create_list(class_type: Any, obj: Optional[Dict[str, Any]], *args) -> List[Any]:
    if obj is None:
        return []
    else:
        return [class_type.from_dict(y, *args) for y in obj]


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
    def from_dict(obj: Any) -> Optional['Column']:
        if obj is None:
            return None
        _name = obj.get("name")
        _dtype = obj.get("dtype")
        return Column(_name, _dtype)


@dataclass
class Attribute:
    name: str
    columns: List[Column]
    separator: str
    is_datetime: bool
    is_compound: bool
    mandatory: bool
    datetime_object: DatetimeObject
    na_rep_columns: List[Column]
    filter_values: List[str]
    use_filter: bool

    @staticmethod
    def from_dict(obj: Any) -> Optional['Attribute']:
        if obj is None:
            return None
        _name = obj.get("name")
        _columns = create_list(Column, obj.get("columns"))
        _is_compound = len(_columns) > 1
        _mandatory = bool(obj.get("mandatory"))
        _datetime_object = DatetimeObject.from_dict(obj.get("datetime_object"))
        _is_datetime = _datetime_object is not None
        _na_rep_columns = create_list(Column, obj.get("na_rep_columns"))
        _separator = obj.get("separator")
        _filter_values = obj.get("filter_values")
        _use_filter = replace_undefined_value(obj.get("use_filter"), _filter_values is not None)
        return Attribute(_name, _columns, _separator, _is_datetime, _is_compound, _mandatory, _datetime_object,
                         _na_rep_columns, _filter_values, _use_filter)


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

    @staticmethod
    def from_dict(obj: Any) -> Optional['Class']:
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
        return Class(_label, _required_attributes, _ids, _df, _include_label_in_cdf, _df_entity_labels, _threshold,
                     _relative_threshold)


@dataclass
class EventAttribute:
    name: str
    values: List[Any]
    is_id: bool

    @staticmethod
    def from_dict(obj: Any) -> Optional['EventAttribute']:
        if obj is None:
            return None
        _name = obj.get("name")
        _values = replace_undefined_value(obj.get("values"), ["IS NOT NULL", '<> "nan"', '<> "None"'])
        if _values != ["IS NOT NULL", '<> "nan"', '<> "None"']:
            _values = [f'''= "{value}"''' for value in _values]
        _is_id = replace_undefined_value(obj.get("is_id"), False)
        return EventAttribute(_name, _values, _is_id)


class Entity:

    def __init__(self, include: bool, label: str, additional_labels: List[str], event_attributes: List[EventAttribute],
                 corr: bool, df: bool, include_label_in_df: bool, merge_duplicate_df: bool, delete_parallel_df: bool):
        self.include = include
        self.label = label
        self.additional_labels = additional_labels
        self.event_attributes = event_attributes
        self.corr = corr
        self.df = df
        self.include_label_in_df = include_label_in_df
        self.merge_duplicate_df = merge_duplicate_df
        self.delete_parallel_df = delete_parallel_df

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

    @staticmethod
    def from_dict(obj: Any) -> Optional['Entity']:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        _label = replace_undefined_value(obj.get("label"), "")
        _additional_labels = replace_undefined_value(obj.get("additional_labels"), [])
        _event_attributes = create_list(EventAttribute, obj.get("event_attributes"))
        _corr = _include or replace_undefined_value(obj.get("corr"), False)
        _df = _corr or replace_undefined_value(obj.get("df"), False)
        _include_label_in_df = _df or replace_undefined_value(obj.get("include_label_in_df"), False)
        _merge_duplicate_df = _include or replace_undefined_value(obj.get("merge_duplicate_df"), False)
        _delete_parallel_df = _include or replace_undefined_value(obj.get("delete_parallel_df"), False)
        return Entity(_include, _label, _additional_labels, _event_attributes, _df, _include_label_in_df, _corr,
                      _merge_duplicate_df,
                      _delete_parallel_df)


@dataclass
class Log:
    include: bool
    has: bool

    @staticmethod
    def from_dict(obj: Any) -> 'Log':
        if obj is None:
            return Log(True, True)
        _include = replace_undefined_value(obj.get("include"), True)
        _has = replace_undefined_value(obj.get("has"), True)
        return Log(_include, _has)


@dataclass
class Relation:
    include: bool
    type: str
    from_node_label: str
    to_node_label: str
    event_reference_attribute: str
    reify: bool
    reified_entity: Entity

    @staticmethod
    def from_dict(obj: Any) -> Optional['Relation']:
        if obj is None:
            return None
        _include = replace_undefined_value(obj.get("include"), True)
        _type = obj.get("type")
        _from_node_label = obj.get("from_node_label")
        _to_node_label = obj.get("to_node_label")
        _event_reference_attribute = obj.get("event_reference_attribute")
        _reified_entity = Entity.from_dict(obj.get("reified_entity"))
        _reify = replace_undefined_value(obj.get("reify"), _reified_entity is not None)
        return Relation(_include, _type, _from_node_label, _to_node_label, _event_reference_attribute, _reify,
                        _reified_entity)


@dataclass
class Sample:
    file_name: str
    use_random_sample: bool
    population_column: str
    size: int
    ids: List[Any]

    @staticmethod
    def from_dict(obj: Any) -> Optional['Sample']:
        if obj is None:
            return None
        _file_name = obj.get("file_name")
        _use_random_sample = obj.get("use_random_sample")
        _population_column = obj.get("population_column")
        _size = obj.get("size")
        _ids = obj.get("ids")
        return Sample(_file_name, _use_random_sample, _population_column, _size, _ids)


class EventTable:
    def __init__(self, name: str, file_directory: str, file_names: List[str], labels: List[str], true_values: List[str],
                 false_values: List[str], samples: Dict[str, Sample], attributes: List[Attribute]):
        self.name = name
        self.file_directory = file_directory
        self.file_names = file_names
        self.labels = labels
        self.true_values = true_values
        self.false_values = false_values
        self.samples = samples
        self.attributes = attributes

    @staticmethod
    def from_dict(obj: Any) -> Optional['EventTable']:
        if obj is None:
            return None

        _name = obj.get("name")
        _file_directory = obj.get("file_directory")
        _file_names = obj.get("file_names")
        _labels = obj.get("labels")
        _true_values = obj.get("true_values")
        _false_values = obj.get("false_values")
        _samples = create_list(Sample, obj.get("samples"))
        _samples = {sample.file_name: sample for sample in _samples}
        _attributes = create_list(Attribute, obj.get("attributes"))
        return EventTable(_name, _file_directory, _file_names, _labels, _true_values, _false_values, _samples,
                          _attributes)

    def get_dtype_dict(self):
        dtypes = {}
        for attribute in self.attributes:
            for column in attribute.columns:
                if column.dtype is not None:
                    if column.name not in dtypes:
                        dtypes[column.name] = column.dtype
                    elif column.dtype != dtypes[column.name]:
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
            # TODO make error
            warnings.warn(f"No sample population has been defined for {file_name}")

        sample = self.samples[file_name]
        sample_column = sample.population_column
        if sample.use_random_sample:
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
            if len(attribute.na_rep_columns) > 0:  # impute values in case of missing values
                df_log = EventTable.replace_nan_values(df_log, attribute)
            if attribute.is_compound:  # create attribute by composing
                df_log = EventTable.create_compound_attribute(df_log, attribute)
            else:  # not compound, check for renaming
                df_log = EventTable.rename_column(df_log, attribute)

        return df_log

    def prepare_data_set(self, input_path, file_name, use_sample):
        dtypes = self.get_dtype_dict()
        required_columns = self.get_required_columns()

        true_values = self.true_values
        false_values = self.false_values

        df_log: DataFrame = pd.read_csv(os.path.realpath(input_path + file_name), keep_default_na=True,
                                        usecols=required_columns, dtype=dtypes, true_values=true_values,
                                        false_values=false_values)

        if use_sample:
            df_log = self.create_sample(file_name, df_log)

        df_log = self.preprocess_according_to_attributes(df_log)

        # all columns have been renamed to or constructed with the name attribute,
        # hence only keep those that match with a name attribute
        required_attributes = list(set([attribute.name for attribute in self.attributes]))
        df_log = df_log[required_attributes]

        return df_log

    def get_datetime_formats(self) -> Dict[str, DatetimeObject]:
        datetime_formats = {}

        for attribute in self.attributes:
            if attribute.is_datetime:
                datetime_formats[attribute.name] = attribute.datetime_object

        return datetime_formats

    def get_attribute_value_pairs_filtered(self) -> Dict[str, List[str]]:
        attribute_value_pairs = {}

        for attribute in self.attributes:
            if attribute.use_filter:
                attribute_value_pairs[attribute.name] = attribute.filter_values

        return attribute_value_pairs


class SemanticHeader:
    def __init__(self, name: str, version: str, sample_seed: int,
                 event_tables: List[EventTable], entities: List[Entity],
                 relations: List[Relation], classes: List[Class],
                 log: Log):
        self.name = name
        self.version = version
        self.sample_seed = sample_seed
        random.seed(self.sample_seed)

        self.event_tables = event_tables
        self.entities = entities
        self.relations = relations
        self.classes = classes
        self.log = log

    def get_entity(self, entity_label) -> Optional['Entity']:
        for entity in self.entities:
            if entity_label == entity.label:
                return entity
        return None

    @staticmethod
    def from_dict(obj: Any) -> Optional['SemanticHeader']:
        if obj is None:
            return None

        _name = obj.get("name")
        _version = obj.get("version")
        _sample_seed = 1
        _event_tables = create_list(EventTable, obj.get("event_tables"))
        _entities = create_list(Entity, obj.get("entities"))
        _relations = create_list(Relation, obj.get("relations"))
        _classes = create_list(Class, obj.get("classes"))
        _log = Log.from_dict(obj.get("log"))
        return SemanticHeader(_name, _version, _sample_seed, _event_tables, _entities, _relations,
                              _classes, _log)
