# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 14:02:29 2019

@author: 20175070
"""
# incidents
import pandas as pd
import math, random
import time, os, csv

from csv_to_eventgraph_neo4j.auxiliary_functions import convert_columns_into_camel_case

# config
sample = False
inputpath = '..\\data\\BPIC14\\'
path_to_neo4j_import_directory = '..\\data\\BPIC14\\prepared\\'  # where prepared files will be stored

start = time.time()


### data prep
sample_file_name = "_sample" if sample else ""


change = pd.read_csv(inputpath + f'Detail_Change.csv', keep_default_na=True, sep=';')
change.columns = convert_columns_into_camel_case(change.columns.values)
change['log'] = 'BPIC14'

incident = pd.read_csv(inputpath + f'Detail_Incident.csv', keep_default_na=True, sep=';')
incident.columns = convert_columns_into_camel_case(incident.columns.values)
incident['log'] = 'BPIC14'

incidentDetail = pd.read_csv(inputpath + f'Detail_Incident_Activity.csv', keep_default_na=True, sep=';')
incidentDetail.columns = convert_columns_into_camel_case(incidentDetail.columns.values)
incidentDetail['log'] = 'BPIC14'

interaction = pd.read_csv(inputpath + f'Detail_Interaction.csv', keep_default_na=True, sep=';')
interaction.columns = convert_columns_into_camel_case(interaction.columns.values)
interaction['log'] = 'BPIC14'

incident = incident.dropna(how='all', axis=1)  # drop all columns in which all values are nan (empty)
incident = incident.dropna(thresh=19)  # drops all 'nan-only' rows

if sample:  ## global sample params
    random.seed(1)
    change = change[change['serviceComponentWbsAff'].isin(
        random.sample(change['serviceComponentWbsAff'].unique().tolist(), 20))]
    incident = incident[incident['serviceComponentWbsAff'].isin(
        random.sample(incident['serviceComponentWbsAff'].unique().tolist(), 20))]
    incidentDetail = incidentDetail[
        incidentDetail['incidentId'].isin(random.sample(incidentDetail['incidentId'].unique().tolist(), 20))]
    interaction = interaction[interaction['serviceCompWbsAff'].isin(
        random.sample(interaction['serviceCompWbsAff'].unique().tolist(), 10))]

# Actual Start (start)/Actual End (timestamp) are not always defined: impute missing values from ChangeRecord attributes
change.loc[change['actualStart'] != change['actualStart'], 'actualStart'] = change.loc[change['actualStart'] != change['actualStart'], 'changeRecordOpenTime']
change.loc[change['actualEnd'] != change['actualEnd'], 'actualEnd'] = change.loc[change['actualEnd'] != change['actualEnd'], 'changeRecordCloseTime']

change = change.reset_index(drop=True)
change.to_csv(path_to_neo4j_import_directory + "BPIC14Change" + sample_file_name + ".csv",
              index=True, index_label="idx")

for i in incident.index:
    incident.at[i, 'activity'] = str(incident.at[i, 'category']) + ": " + str(incident.at[i, 'closureCode'])
incident = incident.replace(['#MULTIVALUE', '#N/B'], 'Unknown')
incident = incident.reset_index(drop=True)
incident.to_csv(path_to_neo4j_import_directory + "BPIC14Incident" + sample_file_name + ".csv",
                index=True, index_label="idx")


incidentDetail = incidentDetail.reset_index(drop=True)
incidentDetail.to_csv(path_to_neo4j_import_directory + "BPIC14IncidentDetail" + sample_file_name + ".csv",
                      index=True, index_label="idx")

for i in interaction.index:
    interaction.at[i, 'activity'] = str(interaction.at[i, 'category']) + ": " + str(interaction.at[i, 'closureCode'])
interaction = interaction.reset_index(drop=True)
interaction = interaction.replace(['#MULTIVALUE', '#N/B'], 'Unknown')
interaction.to_csv(path_to_neo4j_import_directory + "BPIC14Interaction" + sample_file_name + ".csv",
                   index=True, index_label="idx")

end = time.time()
print("Prepared data for import in: " + str((end - start)) + " seconds.")
