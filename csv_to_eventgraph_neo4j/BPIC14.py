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
    step_delete_parallel_df=False,
    step_create_df=True,
    step_create_event_classes=True,
    step_create_dfc=False,
    step_delete_duplicate_df=False,
    option_df_entity_type_in_label=True
)

semantics = Semantics(
    include_entities=['ConfigurationItem', 'ServiceComponent', 'Incident', 'Interaction', 'Change', 'Case_R', 'KM'],
    model_entities=[
        # Configuration Item
        ModelledEntity(entity_label='ConfigurationItem', property_name_id='CINameAff'),
        # Service Component
        ModelledEntity(entity_label='ServiceComponent', property_name_id='ServiceComponentAff'),
        # Incident reported on a configuration item
        ModelledEntity(entity_label='Incident', property_name_id='IncidentID'),
        # Interaction carried out in relation to a configuration item
        ModelledEntity(entity_label='Interaction', property_name_id='InteractionID'),
        ModelledEntity(entity_label='Change', property_name_id='ChangeID'),
        # resource perspective
        ModelledEntity(entity_label='Case_R', property_name_id='Resource'),
        ModelledEntity(entity_label="KM", property_name_id="KMNo")
    ],
    model_relations=[
        Relation(relation_type='RelatedIncident', entity_label_from_node=' Interaction',
                 entity_label_to_node='Incident',
                 reference_in_event_to_to_node='RelatedIncident'),
        Relation(relation_type='PartOf', entity_label_from_node='ConfigurationItem',
                 entity_label_to_node='ServiceComponent',
                 reference_in_event_to_to_node='ServiceComponentAff')
    ],
    classes=[
        Class(label="Event", required_keys=["Activity"], ids=["Name"])
        # Class(label="Event", required_keys=["Resource"], ids=["Name"])
    ],
    dfc_entities=[
        DFC(classifiers=["Activity"])
        # DFC(classifiers=["Resource"], entities=['Application', 'Workflow', 'Offer', 'Case_AO', 'Case_AW', 'Case_WO'])
    ]
)

# region BPIC14 files
BPIC14_full = BPIC(
    name="BPIC14",
    file_names=['BPIC14Change.csv', 'BPIC14Incident.csv', 'BPIC14IncidentDetail.csv', 'BPIC14Interaction.csv'],
    file_type="full",
    data_path='..\\data\\BPIC14\\prepared\\',
    perf_file_path='..\\perf\\BPIC14\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=30
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
    number_of_steps=30
)
# endregion
