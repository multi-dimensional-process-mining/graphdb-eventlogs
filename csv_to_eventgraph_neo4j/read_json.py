import json

from csv_to_eventgraph_neo4j.datatypes import ModelledEntity, Relation, Class, DFC, Semantics, Settings, BPIC, \
    DatetimeObject

with open('BPIC14.json') as f:
    semantic_header = json.load(f)

print(semantic_header)

include_entities = [x['label'] for x in semantic_header['entities']]
model_entities = [ModelledEntity(entity_label=x['label'], property_name_id=x['event_attribute']) for x in semantic_header['entities']]
model_relations = [Relation(relation_type=x['type'], entity_label_from_node=x['from_node_label'], entity_label_to_node=x['to_node_label'],reference_in_event_to_to_node=x['event_reference_attribute']) for x in semantic_header['relations']]
classes = [Class(label=x['label'], required_keys=x['required_attributes'], ids=['ids']) for x in semantic_header['classes']]
dfc_entities = [DFC(classifiers=x['required_attributes']) for x in semantic_header['classes'] if x['DF']]

settings = Settings(
    step_clear_db=True,
    step_populate_graph=True,
    step_load_events_from_csv=True,
    step_filter_events=False,
    step_create_log=True,
    step_create_entities=True,
    step_create_entity_relations=True,
    step_reify_relations=False,
    step_delete_parallel_df=False,
    step_create_df=True,
    step_create_event_classes=True,
    step_create_dfc=False,
    step_delete_duplicate_df=False,
    option_df_entity_type_in_label=True
)

semantics = Semantics(
    include_entities=include_entities,
    model_entities=model_entities,
    model_relations=model_relations,
    classes=classes,
    dfc_entities=dfc_entities
)

mapping_columns_to_property_names = {
    "BPIC14Change": {'changeType': 'activity',  # activity name
                     'actualStart': 'start',  # start timestamp of the activity
                     'actualEnd': 'timestamp',  # complete timestamp of the activity
                     'serviceComponentWbsAff': 'serviceComponentAff',  # sample by
                     # 'changeRecordOpenTime'  # only 2 timestamps with no null values
                     # 'changeRecordCloseTime' # only 2 timestamps with no null values
                     },
    "BPIC14Incident": {'serviceComponentWbsAff': 'serviceComponentAff',  # sample by
                       'openTime': 'start',  # only 2 timestamps with no null values
                       'closeTime': 'timestamp',  # only 2 timestamps with no null values
                       'serviceCompWbsCby': 'serviceComponentCBy'},
    "BPIC14IncidentDetail": {'dateStamp': 'timestamp',  # timestamp
                             'incidentActivityType': 'activity',
                             'assignmentGroup': 'resource'},
    "BPIC14Interaction": {'serviceCompWbsAff': 'serviceComponentAff',  # sample by
                          'openTimeFirstTouch': 'start',  # start timestamp
                          'closeTime': 'timestamp',  # end timestamp
                          }
}




datetime_formats = {
    "BPIC14Change": {
        'start': DatetimeObject(_format='d-M-y H:mX', offset="+01", convert_to="ISO_DATE_TIME"),
        'timestamp': DatetimeObject(_format='d-M-y H:mX', offset="+01", convert_to="ISO_DATE_TIME"),
    },
    "BPIC14Incident": {
        'start': DatetimeObject(_format='d/M/y H:m:sX', offset="+01", convert_to="ISO_DATE_TIME"),
        'timestamp': DatetimeObject(_format='d/M/y H:m:sX', offset="+01", convert_to="ISO_DATE_TIME")
    },
    "BPIC14IncidentDetail":{
        'timestamp': DatetimeObject(_format='d-M-y H:m:sX', offset="+01", convert_to="ISO_DATE_TIME")
    },
    "BPIC14Interaction":{
        'start': DatetimeObject(_format='d-M-y H:mX', offset="+01", convert_to="ISO_DATE_TIME"),
        'timestamp': DatetimeObject(_format='d-M-y H:mX', offset="+01", convert_to="ISO_DATE_TIME"),
    }
}

# region BPIC14 files
BPIC14_full = BPIC(
    name="BPIC14",
    file_names=['BPIC14Change.csv', 'BPIC14Incident.csv', 'BPIC14IncidentDetail.csv', 'BPIC14Interaction.csv'],
    file_type="full",
    data_path='..\\data\\BPIC14\\prepared\\',
    perf_file_path='..\\perf\\BPIC14\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=30,
    mapping=mapping_columns_to_property_names,
    datetime_formats=datetime_formats
)

BPIC14_sample = BPIC(
    name="BPIC14",
    file_names=['BPIC14Change_sample.csv', 'BPIC14Incident_sample.csv', 'BPIC14IncidentDetail_sample.csv',
                'BPIC14Interaction_sample.csv'],
    file_type="sample",
    data_path='..\\data\\BPIC14\\prepared\\',
    perf_file_path='..\\perf\\BPIC14\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=30,
    mapping=mapping_columns_to_property_names,
    datetime_formats=datetime_formats
)