from datatypes import ModelledEntity, Relation, AttributeValuesPair, Class, DFC, BPIC, Settings, Semantics, DatetimeObject

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
    include_entities=['ConfigurationItem', 'ServiceComponent', 'Incident', 'Interaction', 'Change', 'CaseR', 'KM'],
    model_entities=[
        # Configuration Item
        ModelledEntity(entity_label='ConfigurationItem', property_name_id='ciNameAff'),
        # Service Component
        ModelledEntity(entity_label='ServiceComponent', property_name_id='serviceComponentAff'),
        # Incident reported on a configuration item
        ModelledEntity(entity_label='Incident', property_name_id='incidentId'),
        # Interaction carried out in relation to a configuration item
        ModelledEntity(entity_label='Interaction', property_name_id='interactionId'),
        ModelledEntity(entity_label='Change', property_name_id='changeId'),
        # resource perspective
        ModelledEntity(entity_label='CaseR', property_name_id='resource'),
        ModelledEntity(entity_label="KM", property_name_id="kmNumber")
    ],
    model_relations=[
        Relation(relation_type='RELATED_INCIDENT', entity_label_from_node='Interaction',
                 entity_label_to_node='Incident',
                 reference_in_event_to_to_node='relatedIncident'),
        Relation(relation_type='PART_OF', entity_label_from_node='ConfigurationItem',
                 entity_label_to_node='ServiceComponent',
                 reference_in_event_to_to_node='serviceComponentAff')
    ],
    classes=[
        Class(label="Event", required_keys=["activity"], ids=["name"])
        # Class(label="Event", required_keys=["resource"], ids=["Name"])
    ],
    dfc_entities=[
        DFC(classifiers=["activity"])
        # DFC(classifiers=["resource"], entities=['Application', 'Workflow', 'Offer', 'Case_AO', 'Case_AW', 'Case_WO'])
    ]
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
# endregion
