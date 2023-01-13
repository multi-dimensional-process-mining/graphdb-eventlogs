from datatypes import ModelledEntity, Relation, AttributeValuesPair, Class, DFC, BPIC, Settings, Semantics

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
    include_entities=['Customer', 'Office_U', 'Office_W', 'Complaint', 'ComplaintDossier', 'Session', 'IP'],
    # include_entities=['Application'],
    model_entities=[
        # Original Case ID
        ModelledEntity(entity_label='Customer', property_name_id='CustomerID'),
        ModelledEntity(entity_label='Office_U', property_name_id='Office_U'),
        ModelledEntity(entity_label='Office_W', property_name_id='Office_W'),
        ModelledEntity(entity_label='Complaint', property_name_id='ComplaintID'),
        ModelledEntity(entity_label='ComplaintDossier', property_name_id='ComplaintDossierID'),
        ModelledEntity(entity_label='Session', property_name_id='SessionID'),
        ModelledEntity(entity_label='IP', property_name_id='IPID'),
    ],
    classes=[
        Class(label="Event", required_keys=["Activity"], ids=["Name"])
        # Class(label="Event", required_keys=["Resource"], ids=["Name"])
    ],
    # dfc_entities=[
    #     DFC(classifiers=["Activity"])
    #     # DFC(classifiers=["Resource"], entities=['Application', 'Workflow', 'Offer', 'Case_AO', 'Case_AW', 'Case_WO'])
    # ]
)

# region BPIC14 files
BPIC16_full = BPIC(
    name="BPIC16",
    file_names=['BPIC16fullQuestions.csv', 'BPIC16fullMessages.csv', 'BPIC16fullComplaints.csv', 'BPIC16fullClicks.csv'],
    file_type="full",
    data_path='..\\data\\BPIC16\\prepared\\',
    perf_file_path='..\\perf\\BPIC16\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=100
)

BPIC16_sample = BPIC(
    name="BPIC16",
    file_names=['BPIC16sampleQuestions.csv', 'BPIC16sampleMessages.csv', 'BPIC16sampleComplaints.csv', 'BPIC16sampleClicks.csv'],
    file_type="sample",
    data_path='..\\data\\BPIC16\\prepared\\',
    perf_file_path='..\\perf\\BPIC16\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=100
)
# endregion
