import json
import os
import warnings
import random

from typing import List, Dict, Any, Optional
from dataclasses import dataclass

import pandas as pd
from pandas import DataFrame

from a_scripts.additional_functions.auxiliary_functions import replace_undefined_value, create_list


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
    mandatory: bool

    @staticmethod
    def from_dict(obj: Any) -> Optional['Column']:
        if obj is None:
            return None
        _name = obj.get("name")
        _dtype = obj.get("dtype")
        _mandatory = replace_undefined_value(obj.get("mandatory"), True)
        return Column(_name, _dtype, _mandatory)


@dataclass
class Attribute:
    name: str
    columns: List[Column]
    separator: str
    is_datetime: bool
    is_compound: bool
    mandatory: bool
    datetime_object: DatetimeObject
    na_rep_value: Any
    na_rep_columns: List[Column]
    filter_exclude_values: List[str]
    filter_include_values: List[str]
    use_filter: bool
    is_primary_key: bool
    is_foreign_key: bool

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
        _na_rep_value = obj.get("na_rep_value")
        _na_rep_columns = create_list(Column, obj.get("na_rep_columns"))
        _separator = obj.get("separator")
        _filter_exclude_values = obj.get("filter_exclude_values")
        _filter_include_values = obj.get("filter_include_values")
        _use_filter = _filter_include_values is not None or _filter_exclude_values is not None  # default value
        _use_filter = replace_undefined_value(obj.get("use_filter"), _use_filter)
        _is_primary_key = replace_undefined_value(obj.get("is_primary_key"), False)
        _is_foreign_key = replace_undefined_value(obj.get("is_foreign_key"), False)
        return Attribute(_name, _columns, _separator, _is_datetime, _is_compound, _mandatory, _datetime_object,
                         _na_rep_value, _na_rep_columns, _filter_exclude_values, _filter_include_values, _use_filter,
                         _is_primary_key, _is_foreign_key)


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


class StaticNodes:
    def __init__(self, name: str, file_directory: str, file_names: List[str], labels: List[str],
                 true_values: List[str], false_values: List[str], attributes: List[Attribute]):
        self.name = name
        self.file_directory = file_directory
        self.file_names = file_names
        self.labels = labels
        self.true_values = true_values
        self.false_values = false_values
        self.attributes = attributes

    def from_dict(obj: Any) -> Optional['StaticNodes']:
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
        return StaticNodes(_name, _file_directory, _file_names, _labels, _true_values, _false_values, _samples,
                           _attributes)


class DataStructure:
    def __init__(self, include: bool, name: str, file_directory: str, file_names: List[str], labels: List[str],
                 true_values: List[str],
                 false_values: List[str], samples: Dict[str, Sample], attributes: List[Attribute]):
        self.include = include
        self.name = name
        self.file_directory = file_directory
        self.file_names = file_names
        self.labels = labels
        self.true_values = true_values
        self.false_values = false_values
        self.samples = samples
        self.attributes = attributes

    def is_event_data(self):
        return "Event" in self.labels

    @staticmethod
    def from_dict(obj: Any) -> Optional['DataStructure']:
        if obj is None:
            return None

        _include = replace_undefined_value(obj.get("include"), True)

        if not _include:
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
        return DataStructure(_include, _name, _file_directory, _file_names, _labels, _true_values, _false_values,
                             _samples, _attributes)

    def get_primary_keys(self):
        return [attribute.name for attribute in self.attributes if attribute.is_primary_key]

    def get_foreign_keys(self):
        return [attribute.name for attribute in self.attributes if attribute.is_foreign_key]

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
    def replace_nan_values_based_on_na_rep_columns(df_log, attribute):
        if len(attribute.na_rep_columns) != len(attribute.columns):
            # TODO make error
            warnings.warn(
                f"Na_rep_columns does not have the same size as columns for attribute {attribute.name}")
        else:  # they are the same size
            for column, na_rep_column in zip(attribute.columns, attribute.na_rep_columns):
                df_log[column.name].fillna(df_log[na_rep_column.name], inplace=True)

        return df_log

    @staticmethod
    def replace_nan_values_based_on_na_rep_value(df_log, attribute):
        for column in attribute.columns:
            df_log[column.name].fillna(attribute.na_rep_value, inplace=True)

        return df_log

    @staticmethod
    def replace_nan_values_with_unknown(df_log, attribute):
        column: Column
        for column in attribute.columns:
            if column.mandatory:
                df_log[column.name].fillna("Unknown", inplace=True)

        return df_log

    @staticmethod
    def create_compound_attribute(df_log, attribute):
        compound_column_names = [x.name for x in attribute.columns]
        df_log[attribute.name] = df_log[compound_column_names].apply(
            lambda row: attribute.separator.join([value for value in row.values.astype(str) if
                                                  not (value == 'nan' or value != value)]), axis=1)
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
                df_log = DataStructure.replace_nan_values_based_on_na_rep_columns(df_log, attribute)
            if attribute.na_rep_value is not None:
                df_log = DataStructure.replace_nan_values_based_on_na_rep_value(df_log, attribute)
            if attribute.mandatory:
                df_log = DataStructure.replace_nan_values_with_unknown(df_log, attribute)
            if attribute.is_compound:  # create attribute by composing
                df_log = DataStructure.create_compound_attribute(df_log, attribute)
            else:  # not compound, check for renaming
                df_log = DataStructure.rename_column(df_log, attribute)

        return df_log

    def prepare_event_data_sets(self, input_path, file_name, use_sample):
        dtypes = self.get_dtype_dict()
        required_columns = self.get_required_columns()

        true_values = self.true_values
        false_values = self.false_values

        df_log: DataFrame = pd.read_csv(os.path.realpath(input_path + file_name), keep_default_na=True,
                                        usecols=required_columns, dtype=dtypes, true_values=true_values,
                                        false_values=false_values)

        if use_sample and self.is_event_data():
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

    def get_attribute_value_pairs_filtered(self, exclude: bool = True) -> Dict[str, List[str]]:
        attribute_value_pairs = {}

        for attribute in self.attributes:
            if attribute.use_filter:
                attribute_value_pairs[attribute.name] \
                    = attribute.filter_exclude_values if exclude else attribute.filter_include_values

        return attribute_value_pairs


class ImportedDataStructures:
    def __init__(self, dataset_name):
        random.seed(1)
        with open(f'../json_files/{dataset_name}_DS.json') as f:
            json_event_tables = json.load(f)

        self.structures = [DataStructure.from_dict(item) for item in json_event_tables]
