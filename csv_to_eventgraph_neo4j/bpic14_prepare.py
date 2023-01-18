# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 14:02:29 2019

@author: 20175070
"""
# incidents
import pandas as pd
import math, random
import time, os, csv
from re import sub

# config
sample = False
inputpath = '..\\data\\BPIC14\\'
path_to_neo4j_import_directory = '..\\data\\BPIC14\\prepared\\'  # where prepared files will be stored

start = time.time()


def camel_case(s):
    # remove all _ or - and replace by a space
    s = sub(r"(_|-)+", " ", s)
    # Add Space Before Capital Letter If and Only If Previous Letter is Not Also Capital
    # So 'HelloCHARLIE this isBob.' should become 'Hello CHARLIE this is Bob.'
    s = sub(r"(?<![A-Z])(?<!^)([A-Z])", r" \1", s)
    # remove all ( and ) and replace # with num, capitalize each word and remove all spaces
    s = s.replace("(", "").replace(")", "").replace("#", "num").title().replace(" ", "")
    # make first word start with lower case
    return ''.join([s[0].lower(), s[1:]])


def convert_columns_into_camel_case(columns):
    return [camel_case(column) for column in columns]


### data prep

change = pd.read_csv(inputpath + f'Detail_Change.csv', keep_default_na=True, sep=';')
change.columns = convert_columns_into_camel_case(change.columns.values)

incident = pd.read_csv(inputpath + f'Detail_Incident.csv', keep_default_na=True, sep=';')
incident.columns = convert_columns_into_camel_case(incident.columns.values)

incidentDetail = pd.read_csv(inputpath + f'Detail_Incident_Activity.csv', keep_default_na=True, sep=';')
incidentDetail.columns = convert_columns_into_camel_case(incidentDetail.columns.values)

interaction = pd.read_csv(inputpath + f'Detail_Interaction.csv', keep_default_na=True, sep=';')
interaction.columns = convert_columns_into_camel_case(interaction.columns.values)

incident.drop(incident.iloc[:, 28:78], inplace=True, axis=1)  # drop all empty columns
incident = incident.dropna(thresh=19)  # drops all 'nan-only' rows

change['log'] = 'BPIC14'
incident['log'] = 'BPIC14'
incidentDetail['log'] = 'BPIC14'
interaction['log'] = 'BPIC14'

if sample:  ## global sample params
    random.seed(1)
    change = change[change['serviceComponentWbsAff'].isin(
        random.sample(change['serviceComponentWbsAff'].unique().tolist(), 20))]
    incident = incident[incident['serviceComponentWBSAff'].isin(
        random.sample(incident['serviceComponentWBSAff'].unique().tolist(), 20))]
    incidentDetail = incidentDetail[
        incidentDetail['incidentId'].isin(random.sample(incidentDetail['incidentId'].unique().tolist(), 20))]
    interaction = interaction[interaction['serviceCompWBSAff'].isin(
        random.sample(interaction['serviceCompWBSAff'].unique().tolist(), 10))]

# Actual Start (start)/Actual End (timestamp) are not always defined: impute missing values from ChangeRecord attributes
for i in change.index:
    if change.at[i, 'actualStart'] != change.at[i, 'actualStart']:
        change.at[i, 'actualStart'] = change.at[i, 'changeRecordOpenTime']
    if change.at[i, 'actualEnd'] != change.at[i, 'actualEnd']:
        change.at[i, 'actualEnd'] = change.at[i, 'changeRecordCloseTime']

change['actualStart'] = pd.to_datetime(change['actualStart'], format='%d-%m-%Y %H:%M')
change['actualStart'] = change['actualStart'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M') + ':00.000+0100')
change['actualEnd'] = pd.to_datetime(change['actualEnd'], format='%d-%m-%Y %H:%M')
change['actualEnd'] = change['actualEnd'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M') + ':00.000+0100')

change = change.reset_index(drop=True)

if sample:  # sample params for log file
    fileName = 'BPIC14Change_sample.csv'
else:
    fileName = 'BPIC14Change.csv'

change.to_csv(path_to_neo4j_import_directory + fileName, index=True, index_label="idx", na_rep="Unknown")

for i in incident.index:
    incident.at[i, 'activity'] = str(incident.at[i, 'category']) + ": " + str(incident.at[i, 'closureCode'])

incident = incident.replace(['#MULTIVALUE', '#N/B'], 'Unknown')
incident['openTime'] = pd.to_datetime(incident['openTime'], format='%d/%m/%Y %H:%M:%S')
incident['openTime'] = incident['openTime'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + '.000+0100')
incident['closeTime'] = pd.to_datetime(incident['closeTime'], format='%d/%m/%Y %H:%M:%S')
incident['closeTime'] = incident['closeTime'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + '.000+0100')
incident = incident.reset_index(drop=True)
if sample:  # sample params for log file
    fileName = 'BPIC14Incident_sample.csv'
else:
    fileName = 'BPIC14Incident.csv'

incident.to_csv(path_to_neo4j_import_directory + fileName, index=True, index_label="idx", na_rep="Unknown")

incidentDetail['dateStamp'] = pd.to_datetime(incidentDetail['dateStamp'], format='%d-%m-%Y %H:%M:%S')
incidentDetail['dateStamp'] = incidentDetail['dateStamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + '.000+0100')
incidentDetail = incidentDetail.reset_index(drop=True)
if sample:  # sample params for log file
    fileName = 'BPIC14IncidentDetail_sample.csv'
else:
    fileName = 'BPIC14IncidentDetail.csv'

incidentDetail.to_csv(path_to_neo4j_import_directory + fileName, index=True, index_label="idx", na_rep="Unknown")

for i in interaction.index:
    interaction.at[i, 'activity'] = str(interaction.at[i, 'category']) + ": " + str(interaction.at[i, 'closureCode'])

interaction['openTimeFirstTouch'] = interaction['openTimeFirstTouch'].astype('datetime64[ns]')
interaction['openTimeFirstTouch'] = interaction['openTimeFirstTouch'].map(
    lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + '.000+0100')
interaction['closeTime'] = interaction['closeTime'].astype('datetime64[ns]')
interaction['closeTime'] = interaction['closeTime'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S') + '.000+0100')
interaction = interaction.reset_index(drop=True)
interaction = interaction.replace(['#MULTIVALUE', '#N/B'], 'Unknown')
if sample:  # sample params for log file
    fileName = 'BPIC14Interaction_sample.csv'
else:
    fileName = 'BPIC14Interaction.csv'

interaction.to_csv(path_to_neo4j_import_directory + fileName, index=True, index_label="idx", na_rep="Unknown")

end = time.time()
print("Prepared data for import in: " + str((end - start)) + " seconds.")
