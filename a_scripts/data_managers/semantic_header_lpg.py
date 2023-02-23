from dataclasses import dataclass
from typing import Optional, Any, Self

from a_scripts.data_managers.semantic_header import SemanticHeader, Entity, Relation, Class, Log, Condition


@dataclass
class ConditionLPG(Condition):
    @classmethod
    def from_dict(cls, obj: Any, not_exist_properties=None) -> Optional[Self]:
        if not_exist_properties is None:
            not_exist_properties = ["IS NOT NULL", '<> "nan"', '<> "None"']
        return super().from_dict(obj, not_exist_properties)

    def get_values(self):
        if self.values != ["IS NOT NULL", '<> "nan"', '<> "None"']:
            return [f'''= "{include_value}"''' for include_value in self.values]


class RelationLPG(Relation):
    pass


@dataclass
class EntityLPG(Entity):
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

    def get_composed_primary_id(self, node_name: str = "e"):
        return "+\"-\"+".join([f"{node_name}.{key}" for key in self.primary_keys])

    def get_entity_attributes(self, node_name: str = "e"):
        primary_key_list = [f"{node_name}.{key} as {key}" for key in self.primary_keys]
        entity_attribute_list = [f"COLLECT(distinct {node_name}.{attr}) as {attr}" for attr in
                                 self.entity_attributes_wo_primary_keys]
        complete_list = primary_key_list + entity_attribute_list
        return ','.join(complete_list)

    def get_entity_attributes_as_node_properties(self):
        return ',\n'.join([f"{key}: {key}" for key in self.all_entity_attributes])

    def get_primary_key_existing_condition(self, node_name: str = "e"):
        return " AND ".join(
            [f'''{node_name}.{key} IS NOT NULL AND {node_name}.{key} <> "nan" AND {node_name}.{key}<> "None"''' for key
             in self.primary_keys])

    def create_condition(self, name: str) -> str:
        """
        Converts a dictionary into a string that can be used in a WHERE statement to find the correct node/relation
        @param name: str, indicating the name of the node/rel
        @param properties: Dictionary containing of property name and value
        @return: String that can be used in a where statement
        """

        condition: ConditionLPG
        condition_list = []
        for condition in self.constructed_by.conditions:
            attribute_name = condition.attribute
            include_values = condition.values
            for value in include_values:
                condition_list.append(f'''{name}.{attribute_name} = "{value}"''')
        condition_string = " AND ".join(condition_list)
        return condition_string

    def get_where_condition(self, node_name: str = "e"):
        primary_key_existing_condition = self.get_primary_key_existing_condition(node_name)
        extra_conditions = self.create_condition(node_name)
        if extra_conditions != "":
            return f'''{primary_key_existing_condition} AND {extra_conditions}'''
        else:
            return primary_key_existing_condition

    @classmethod
    def from_dict(cls, obj: Any, condition_class_name: Condition = ConditionLPG,
                  relation_class_name: Relation = RelationLPG) -> Optional[Self]:
        return super().from_dict(obj, condition_class_name, relation_class_name)


class ClassLPG(Class):
    def get_condition(self, node_name="e"):
        # reformat to where e.key is not null to create with condition
        return " AND ".join([f"{node_name}.{key} IS NOT NULL" for key in self.class_identifiers])

    def get_group_by_statement(self, node_name="e"):
        # reformat to e.key with alias to create with condition
        return 'distinct ' + ' , '.join([f"{node_name}.{key} AS {key}" for key in self.class_identifiers])

    def get_class_properties(self) -> str:
        ids = self.class_identifiers
        if "cID" not in ids:
            ids = ["cID"] + ids

        # create a combined id in string format
        _id = "+".join([f"{key}" for key in self.class_identifiers])
        # add to the keys
        required_keys = [_id] + self.class_identifiers

        node_properties = ', '.join([f"{_id}: {key}" for _id, key in zip(ids, required_keys)])
        node_properties += f", classType: '{_id}'"  # save ID also as string that captures the type

        return node_properties

    def get_link_condition(self, class_node_name="c", event_node_name = "e"):
        return ' AND '.join([f"{class_node_name}.{key} = {event_node_name}.{key}" for key in self.class_identifiers])

    def get_class_label(self):
        return "_".join([f"{key}" for key in self.class_identifiers])


class LogLPG(Log):
    pass


class SemanticHeaderLPG(SemanticHeader):
    @classmethod
    def from_dict(cls, obj: Any, derived_entity_class_name: Entity = EntityLPG,
                  reified_entity_class_name: Entity = EntityLPG,
                  relation_class_name: Relation = RelationLPG,
                  class_class_name: Class = ClassLPG,
                  log_class_name: Log = LogLPG) -> Optional[Self]:
        return super().from_dict(obj, derived_entity_class_name=derived_entity_class_name,
                                 reified_entity_class_name=reified_entity_class_name,
                                 relation_class_name=relation_class_name,
                                 class_class_name=class_class_name,
                                 log_class_name=log_class_name)
