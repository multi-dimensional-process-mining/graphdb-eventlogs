# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 14:02:29 2019

@author: 20175070
"""
import numpy as np
# incidents
import pandas as pd
import time

from a_scripts.additional_functions.auxiliary_functions import convert_columns_into_camel_case

# config
inputpath = '../../data/BPIC14\\'
path_to_neo4j_import_directory = '../../data/BPIC14/prepared\\'  # where prepared files will be stored

start = time.time()

change = pd.read_csv(inputpath + f'Detail_Change.csv', keep_default_na=True, sep=';')
change.columns = convert_columns_into_camel_case(change.columns.values)
change['log'] = 'BPIC14'
change = change.reset_index(drop=True)
change.to_csv(path_to_neo4j_import_directory + "BPIC14Change.csv",
              index=True, index_label="idx")

incident = pd.read_csv(inputpath + f'Detail_Incident.csv', keep_default_na=True, sep=';', decimal=",",
                       dtype={"Urgency": "str"})
incident.columns = convert_columns_into_camel_case(incident.columns.values)
# only keep numeric values for urgency column and convert to Int64
incident["urgency"] = incident["urgency"].str.replace('(\D+)', '', regex=True)
incident["urgency"] = incident["urgency"].astype('Int64')
incident['log'] = 'BPIC14'
incident = incident.dropna(how='all', axis=1)  # drop all columns in which all values are nan (empty)
incident = incident.dropna(thresh=19)  # drops all 'nan-only' rows
incident = incident.replace(['#MULTIVALUE', '#N/B'], np.NaN)
incident = incident.reset_index(drop=True)
incident.to_csv(path_to_neo4j_import_directory + "BPIC14Incident.csv",
                index=True, index_label="idx")

incidentDetail = pd.read_csv(inputpath + f'Detail_Incident_Activity.csv', keep_default_na=True, sep=';')
incidentDetail.columns = convert_columns_into_camel_case(incidentDetail.columns.values)
incidentDetail['log'] = 'BPIC14'
incidentDetail = incidentDetail.reset_index(drop=True)
incidentDetail.to_csv(path_to_neo4j_import_directory + "BPIC14IncidentDetail.csv",
                      index=True, index_label="idx")

interaction = pd.read_csv(inputpath + f'Detail_Interaction.csv', keep_default_na=True, sep=';',
                          dtype={"Urgency": "str"})
interaction.columns = convert_columns_into_camel_case(interaction.columns.values)
# only keep numeric values for urgency column and convert to Int64
interaction["urgency"] = interaction["urgency"].str.replace('(\D+)', '', regex=True)
interaction["urgency"] = interaction["urgency"].astype('Int64')
interaction['log'] = 'BPIC14'
interaction = interaction.reset_index(drop=True)
interaction = interaction.replace(['#MULTIVALUE', '#N/B'], np.NaN)
interaction.to_csv(path_to_neo4j_import_directory + "BPIC14Interaction.csv",
                   index=True, index_label="idx")

end = time.time()
print("Prepared data for import in: " + str((end - start)) + " seconds.")
