from datatypes import ModelledEntity, Relation, AttributeValuesPair, Class, DFC, BPIC, Settings, Semantics

settings = Settings(
    step_clear_db=True,
    step_populate_graph=True,
    step_load_events_from_csv=True,
    step_filter_events=False,
    step_create_log=True,
    step_create_entities=True,
    step_create_entity_relations=True,
    step_reify_relations=False,
    step_create_df=True,
    step_delete_parallel_df=False,
    step_create_event_classes=True,
    step_create_dfc=False,
    step_delete_duplicate_df=False,
    option_df_entity_type_in_label=True
)

semantics = Semantics(
    include_entities=['Application', 'CaseR', 'ResponsibleActor', 'MonitoringResource'],
    # include_entities=['Application'],
    model_entities=[
        # Original Case ID
        ModelledEntity(entity_label='Application', property_name_id='caseId'),
        ModelledEntity(entity_label='CaseR', property_name_id='resource'),
        ModelledEntity(entity_label='ResponsibleActor', property_name_id='responsibleActor'),
        ModelledEntity(entity_label='MonitoringResource', property_name_id='monitoringResource')
    ],
    model_relations=[
        Relation(relation_type='SAME_RESOURCE', entity_label_from_node='ResponsibleActor',
                 entity_label_to_node='CaseR',
                 reference_in_event_to_to_node='responsibleActor'),
        Relation(relation_type='SAME_RESOURCE', entity_label_from_node='MonitoringResource',
                 entity_label_to_node='CaseR',
                 reference_in_event_to_to_node='monitoringResource')
    ],
    classes=[
        Class(label="Event", required_keys=["activity"], ids=["name"])
    ]
    # dfc_entities=[
    #     DFC(classifiers=["activity"])
    #     # DFC(classifiers=["Resource"], entities=['Application', 'Workflow', 'Offer', 'Case_AO', 'Case_AW', 'Case_WO'])
    # ]
)

mapping_columns_to_property_names = {
    'BPIC15_1': {'case': 'caseId',
                 'event': 'activity',
                 'orgResource': 'resource',
                 'startTime': 'start',
                 'completeTime': 'timestamp'},
    'BPIC15_2': {'case': 'caseId',
                 'event': 'activity',
                 'orgResource': 'resource',
                 'startTime': 'start',
                 'completeTime': 'timestamp'},
    'BPIC15_3': {'case': 'caseId',
                 'event': 'activity',
                 'orgResource': 'resource',
                 'startTime': 'start',
                 'completeTime': 'timestamp'},
    'BPIC15_4': {'case': 'caseId',
                 'event': 'activity',
                 'orgResource': 'resource',
                 'startTime': 'start',
                 'completeTime': 'timestamp'},
    'BPIC15_5': {'case': 'caseId',
                 'event': 'activity',
                 'orgResource': 'resource',
                 'startTime': 'start',
                 'completeTime': 'timestamp'}
}

# region BPIC14 files
BPIC15_full = BPIC(
    name="BPIC15",
    file_names=['BPIC15_1.csv', 'BPIC15_2.csv', 'BPIC15_3.csv', 'BPIC15_4.csv', 'BPIC15_5.csv'],
    file_type="full",
    data_path='..\\data\\BPIC15\\prepared\\',
    perf_file_path='..\\perf\\BPIC15\\',
    na_values="Unknown",
    dtype_dict = {"responsibleActor": "Int64"},
    settings=settings,
    semantics=semantics,
    number_of_steps=18,
    mapping=mapping_columns_to_property_names
)

BPIC15_sample = BPIC(
    name="BPIC15",
    file_names=['BPIC15_1_sample.csv', 'BPIC15_2_sample.csv', 'BPIC15_3_sample.csv', 'BPIC15_4_sample.csv',
                'BPIC15_5_sample.csv'],
    file_type="sample",
    data_path='..\\data\\BPIC15\\prepared\\',
    perf_file_path='..\\perf\\BPIC15\\',
    na_values="Unknown",
    settings=settings,
    semantics=semantics,
    number_of_steps=18,
    mapping=mapping_columns_to_property_names
)
# endregion
