from datatypes import ModelledEntity, Relation, AttributeValuesPair, Class, DFC, BPIC, Settings, Semantics, \
    DatetimeObject

settings = Settings(
    step_clear_db=True,
    step_populate_graph=True,
    step_load_events_from_csv=True,
    step_filter_events=False,
    step_create_log=True,
    step_create_entities=True,
    step_create_entity_relations=False,
    step_reify_relations=False,
    step_create_df=True,
    step_delete_parallel_df=False,
    step_create_event_classes=True,
    step_create_dfc=False,
    step_delete_duplicate_df=False,
    option_df_entity_type_in_label=True
)

semantics = Semantics(
    include_entities=['Customer', 'OfficeU', 'OfficeW', 'Complaint', 'ComplaintDossier', 'Session', 'IP'],
    # include_entities=['Application'],
    model_entities=[
        # Original Case ID
        ModelledEntity(entity_label='Customer', property_name_id='customerId'),
        ModelledEntity(entity_label='OfficeU', property_name_id='officeU'),
        ModelledEntity(entity_label='OfficeW', property_name_id='officeW'),
        ModelledEntity(entity_label='Complaint', property_name_id='complaintId'),
        ModelledEntity(entity_label='ComplaintDossier', property_name_id='complaintDossierId'),
        ModelledEntity(entity_label='Session', property_name_id='sessionId'),
        ModelledEntity(entity_label='IP', property_name_id='ipid'),
    ],
    classes=[
        Class(label="Event", required_keys=["activity"], ids=["name"])
        # Class(label="Event", required_keys=["Resource"], ids=["Name"])
    ]
)

mapping_columns_to_property_names = {
    "BPIC16Complaints": {
        'contactDate': 'timestamp',
        'complaintTheme': 'activity'},
    "BPIC16Questions": {
        'end': 'timestamp',
        'questionTheme': 'activity'},
    "BPIC16Messages": {
        'eventDateTime': 'timestamp',
        'eventType': 'activity'},
    "BPIC16Clicks": {
        'timestamp': 'timestamp',
        'pageName': 'activity'}
}

datetime_formats = {
    "BPIC16Complaints": {
        'timestamp': DatetimeObject(_format='y-M-d', offset="", convert_to="ISO_DATE"),
    },
    "BPIC16Questions": {
        'start': DatetimeObject(_format='y-M-d H:m:s.nX', offset="+01", convert_to="ISO_DATE_TIME"),
        'timestamp': DatetimeObject(_format='y-M-d H:m:s.nX', offset="+01", convert_to="ISO_DATE_TIME")
    },
    "BPIC16Messages": {
        'timestamp': DatetimeObject(_format='y-M-d H:m:s.nX', offset="+01", convert_to="ISO_DATE_TIME")
    },
    "BPIC16Clicks": {
        'timestamp': DatetimeObject(_format='y-M-d H:m:s.nX', offset="+01", convert_to="ISO_DATE_TIME"),
    }
}

# region BPIC14 files
BPIC16_full = BPIC(
    name="BPIC16",
    file_names=['BPIC16Clicks.csv', 'BPIC16Questions.csv', 'BPIC16Messages.csv', 'BPIC16Complaints.csv'],
    file_type="full",
    data_path='..\\data\\BPIC16\\prepared\\',
    perf_file_path='..\\perf\\BPIC16\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=100,
    mapping=mapping_columns_to_property_names,
    datetime_formats=datetime_formats
)

BPIC16_sample = BPIC(
    name="BPIC16",
    file_names=['BPIC16Questions_sample.csv', 'BPIC16Messages_sample.csv', 'BPIC16Complaints_sample.csv',
                'BPIC16Clicks_sample.csv'],
    file_type="sample",
    data_path='..\\data\\BPIC16\\prepared\\',
    perf_file_path='..\\perf\\BPIC16\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=100,
    mapping=mapping_columns_to_property_names,
    datetime_formats=datetime_formats
)
# endregion
