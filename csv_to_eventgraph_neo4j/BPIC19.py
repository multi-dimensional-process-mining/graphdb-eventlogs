from datatypes import ModelledEntity, Relation, AttributeValuesPair, Class, DFC, BPIC, Settings, Semantics

settings = Settings(
    step_clear_db=True, #True
    step_populate_graph=True,
    step_load_events_from_csv=True, #True
    step_filter_events=False,
    step_create_log=True, #True
    step_create_entities=True, #True
    step_create_entity_relations=True, #True
    step_reify_relations=False,
    step_create_df=True, #True
    step_delete_parallel_df=False,
    step_create_event_classes=True,
    step_create_dfc=False,
    step_delete_duplicate_df=False,
    option_df_entity_type_in_label=True
)

semantics = Semantics(
    # include_entities=['POItem', 'PO', 'Resource', 'Vendor'],
    include_entities=['POItem', 'PO'],
    model_entities=[
        # Original Case ID
        ModelledEntity(entity_label='POItem', property_name_id='cID'),# Purchase Order Items (Original Case ID)
        ModelledEntity(entity_label='PO', property_name_id='cPOID'), # Purchase Orders
        ModelledEntity(entity_label='Resource', property_name_id='resource'),  # resources/users
        ModelledEntity(entity_label='Vendor', property_name_id='cVendor')
    ],
    model_relations=[
        Relation(relation_type='PO', entity_label_from_node='POItem',
                 entity_label_to_node='PO',
                 reference_in_event_to_to_node='cPOID')
    ],
    classes=[
        Class(label="Event", required_keys=["Activity"], ids=["Name"])
        # Class(label="Event", required_keys=["Resource"], ids=["Name"])
    ]
)

# region BPIC19 files
BPIC19_full = BPIC(
    name="BPIC19",
    file_names=['BPIC19full.csv'],
    file_type="full",
    data_path='..\\data\\BPIC19\\prepared\\',
    perf_file_path='..\\perf\\BPIC19\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=100
)

BPIC19_sample = BPIC(
    name="BPIC19",
    file_names=['BPIC19sample.csv'],
    file_type="sample",
    data_path='..\\data\\BPIC19\\prepared\\',
    perf_file_path='..\\perf\\BPIC19\\',
    settings=settings,
    semantics=semantics,
    number_of_steps=100
)
# endregion
