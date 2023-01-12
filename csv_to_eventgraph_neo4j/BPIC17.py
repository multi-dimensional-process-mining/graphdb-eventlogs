from datatypes import ModelledEntity, Relation, AttributeValuesPair, Class, DFC, BPIC, Settings, Semantics

settings = Settings(
    # several steps of import, each can be switch on/off
    step_clear_db=True,
    step_populate_graph=True,
    step_load_events_from_csv=True,
    step_filter_events=True,
    step_create_log=True,
    step_create_entities=True,
    step_create_entity_relations=True,
    step_reify_relations=True,
    step_create_df=True,
    step_delete_parallel_df=False,
    step_create_event_classes=True,
    step_create_dfc=True,
    step_delete_duplicate_df=True,
    option_df_entity_type_in_label=True
)

semantics = Semantics(
    include_entities=['Application', 'Workflow', 'Offer', 'Case_R', 'Case_AO', 'Case_AW', 'Case_WO'],
    # include_entities = ['Application','Workflow','Offer','Case_R','Case_AO','Case_AW','Case_WO','Case_AWO']
    # individual entities
    model_entities=[
        ModelledEntity(entity_label='Application', property_name_id='case',
                       properties={"EventOrigin": ['= "Application"']}),
        ModelledEntity(entity_label='Workflow', property_name_id='case',
                       properties={"EventOrigin": ['= "Workflow"']}),
        ModelledEntity(entity_label='Offer', property_name_id='OfferID',
                       properties={"EventOrigin": ['= "Offer"']}),
        # resource as entity
        ModelledEntity(entity_label='Case_R', property_name_id='resource'),
        # original case notion
        ModelledEntity(entity_label='Case_AWO', property_name_id='case')
    ],
    model_relations=[
        Relation(relation_type='Case_AO', entity_label_from_node='Offer', entity_label_to_node='Application',
                 reference_in_event_to_to_node='case'),
        Relation(relation_type='Case_AW', entity_label_from_node='Workflow', entity_label_to_node='Application',
                 reference_in_event_to_to_node='case'),
        Relation(relation_type='Case_WO', entity_label_from_node='Offer', entity_label_to_node='Workflow',
                 reference_in_event_to_to_node='case')
    ],
    model_entities_derived=['Case_AO', 'Case_AW', 'Case_WO'],
    properties_of_events_to_be_filtered=[
        AttributeValuesPair(attribute='lifecycle', values=["SUSPEND", "RESUME"])
    ],
    classes=[
        Class(label="Event", required_keys=["Activity", "lifecycle"], ids=["Name", "lifecycle"]),
        Class(label="Event", required_keys=["resource"], ids=["Name"])
    ],
    dfc_entities=[
        DFC(classifiers=["Activity", "lifecycle"]),
        DFC(classifiers=["resource"], entities=['Application', 'Workflow', 'Offer', 'Case_AO', 'Case_AW', 'Case_WO'])
    ]

)

# region BPIC17 files
BPIC17_full = BPIC(
    name=" BPIC17",
    file_names=["BPIC17full.csv"],
    file_type="full",
    data_path='..\\data\\BPIC17\\',
    perf_file_path='..\\perf\\BPIC17\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=100,
)

BPIC17_sample = BPIC(
    name=" BPIC17",
    file_names=["BPIC17sample.csv"],
    file_type="sample",
    data_path='..\\data\\BPIC17\\',
    perf_file_path='..\\perf\\BPIC17\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=26,

)
# endregion

