from typing import Dict, List, Type


class ModelledEntity:
    def __init__(self, entity_label: str, property_name_id: str, properties: Dict[str, List[str]] = None):
        self.entity_label = entity_label
        self.property_name_id = property_name_id
        self.properties = properties

        if self.properties is None:
            # if none, then we check whether the property name id exists
            self.properties = {self.property_name_id: ["IS NOT NULL", '<> "nan"', '<> "None"']}


class Relation:
    # specification of relations between entities
    #    1 name of the relation
    #    2 name of first entity,
    #    3 name of second entity where events have an property referring to the first entity, i.e., a foreign key
    #    4 name of the foreign key property by which events of the second entity refer to the first entity
    def __init__(self, relation_type: str, entity_label_from_node: str, entity_label_to_node: str,
                 reference_in_event_to_to_node: str):
        self.relation_type = relation_type
        self.entity_label_to_node = entity_label_to_node
        self.entity_label_from_node = entity_label_from_node
        self.reference_in_event_to_to_node = reference_in_event_to_to_node


class AttributeValuesPair:
    def __init__(self, attribute: str, values: List[str]):
        self.attribute = attribute
        self.values = values


class Class:
    def __init__(self, label: str, required_keys: List[str], ids: List[str]):
        self.label = label
        self.required_keys = required_keys
        self.ids = ids


class DFC:
    def __init__(self, classifiers, entities=None):
        self.classifiers = classifiers
        self.entities = entities


class Semantics:
    def __init__(self, include_entities: List[str], model_entities: List[ModelledEntity],
                 model_relations: List[Relation] = None, model_entities_derived: List[str] = None,
                 properties_of_events_to_be_filtered: List[AttributeValuesPair] = None,
                 classes: List[Class] = None, dfc_entities: List[DFC] = None
                 ) -> None:

        self.include_entities = include_entities
        self.model_entities = model_entities

        self.model_relations = model_relations
        # specification of entities to derive by reifying relations:
        #    1 name of the relation in 'model_relations' that shall be reified
        self.model_entities_derived = Semantics.create_empty_list_if_none(model_entities_derived)
        self.properties_of_events_to_be_filtered = Semantics.create_empty_list_if_none(
            properties_of_events_to_be_filtered)
        self.classes = Semantics.create_empty_list_if_none(classes)
        self.dfc_entities = Semantics.create_empty_list_if_none(dfc_entities)
        self.check_dfc_entities()

    @staticmethod
    def create_empty_list_if_none(list_of_objects):
        return [] if list_of_objects is None else list_of_objects

    def check_dfc_entities(self):
        for dfc_entity in self.dfc_entities:
            if dfc_entity.entities is None:
                dfc_entity.entities = self.include_entities


class Settings:
    def __init__(self,
                 step_clear_db: bool = True, step_populate_graph: bool = True,
                 step_load_events_from_csv: bool = True, step_filter_events: bool = False,
                 step_create_log: bool = True, step_create_entities: bool = True,
                 step_create_entity_relations: bool = True, step_reify_relations: bool = False,
                 step_create_df: bool = True, step_delete_parallel_df: bool = False,
                 step_create_event_classes: bool = True, step_create_dfc: bool = True,
                 step_delete_duplicate_df: bool = False, option_df_entity_type_in_label: bool = True):
        """

        :param step_clear_db: entire graph shall be cleared before starting a new import
        :param step_populate_graph: entire graph shall be populated
        :param step_load_events_from_csv:  import all (new) events from CSV file
        :param step_filter_events: filter events prior to graph construction
        :param step_create_log: create log nodes and relate events to log node
        :param step_create_entities: create entities from identifiers in the data as specified in this script
        :param step_create_entity_relations: create foreign-key relations between entities
        :param step_reify_relations: reify relations into derived entities
        :param step_create_df: compute directly-follows relation for all entities in the data
        :param step_delete_parallel_df: remove directly-follows relations for derived entities that run
                in parallel with DF-relations for base entities
        :param step_create_event_classes: aggregate events to event classes from data
        :param step_create_dfc: aggregate directly-follows relation to event classes
        :param step_delete_duplicate_df delete duplicate df in case of batching

        :param option_df_entity_type_in_label  whether to include df entity type in label
        """
        # several steps of import, each can be switch on/off
        self.step_clear_db = step_clear_db
        self.step_populate_graph = step_populate_graph

        # won't clear the graph if it is not populated again
        self.step_clear_db = self.step_populate_graph & self.step_clear_db

        self.step_load_events_from_csv = step_load_events_from_csv
        self.step_filter_events = step_filter_events
        self.step_create_log = step_create_log
        self.step_create_entities = step_create_entities
        self.step_create_entity_relations = step_create_entity_relations
        self.step_reify_relations = step_reify_relations
        self.step_create_df = step_create_df
        self.step_delete_parallel_df = step_delete_parallel_df
        self.step_create_event_classes = step_create_event_classes
        self.step_create_dfc = step_create_dfc
        self.step_delete_duplicate_df = step_delete_duplicate_df

        self.option_df_entity_type_in_label = option_df_entity_type_in_label


class BPIC:
    def __init__(self, name: str, file_names: List[str], file_type: str, data_path: str, perf_file_path: str,
                 semantics: Semantics, settings: Settings, number_of_steps: int, na_values:str = None, dtype_dict:Dict[str, str] = None):

        self.name = name
        self.file_names = file_names
        self.file_type = file_type
        self.data_path = data_path
        self.perf_file_name = f"{self.name}{self.file_type}Performance.csv"
        self.perf_file_path = perf_file_path

        self.settings = settings
        self.semantics = semantics
        self.number_of_steps = number_of_steps
        self.na_values = na_values
        self.dtype_dict = dtype_dict
