# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 16:51:36 2019

@author: 20175070
"""

# website click data, labour services process
import pandas as pd
import time, os, csv

from csv_to_eventgraph_neo4j.auxiliary_functions import convert_columns_into_camel_case

# config
sample = False
input_path = '..\\data\\BPIC16\\'
output_path = '..\\data\\BPIC16\\prepared\\'  # where prepared files will be stored
file_name = 'BPIC16'


################## data prep ##################

def create_bpic16():
    clicks_log = pd.read_csv(os.path.realpath(input_path + 'BPI2016_Clicks_Logged_In.csv'), keep_default_na=True,
                             sep=';', encoding='latin1')
    clicks_log.columns = convert_columns_into_camel_case(clicks_log.columns.values)
    if sample:
        sample_ids = [2026796, 2223803, 2023026, 114939, 2011721, 2022933, 919259, 2079086, 466152, 2057965, 1039204,
                      395673, 1710155, 2081135, 1723340, 1893155, 1042998, 435939, 1735039, 2045407]
        sample_string = "_sample"
    else:
        sample_ids = clicks_log['customerId'].unique().tolist()
        sample_string = ""

    clicks_log = clicks_log[clicks_log['customerId'].isin(sample_ids)]
    clicks_log.fillna(0)
    clicks_log = clicks_log.drop(columns=list(clicks_log.columns[range(-1, -10, -1)]))
    clicks_log['timestamp'] = pd.to_datetime(clicks_log['timestamp'], format='%Y-%m-%d %H:%M:%S.%f')
    clicks_log['timestamp'] = clicks_log['timestamp'].map(lambda x: x.strftime('%Y-%m-%d %H:%M:%S.%f'))
    clicks_log['log'] = 'BPIC16'
    clicks_log.to_csv(output_path + file_name + 'Clicks' + sample_string + '.csv', index=True, index_label="idx")

    complaints = pd.read_csv(os.path.realpath(input_path + 'BPI2016_Complaints.csv'), keep_default_na=True, sep=';',
                             encoding='latin1')
    complaints.columns = convert_columns_into_camel_case(complaints.columns.values)
    complaints = complaints[complaints['customerId'].isin(sample_ids)]
    complaints.fillna(0)
    complaints['log'] = 'BPIC16'
    complaints.to_csv(output_path + file_name + 'Complaints' + sample_string + '.csv', index=True, index_label="idx",
                      na_rep="Unknown")

    questions = pd.read_csv(os.path.realpath(input_path + 'BPI2016_Questions.csv'), keep_default_na=True, sep=';',
                            encoding='latin1')
    questions.columns = convert_columns_into_camel_case(questions.columns.values)
    questions = questions[questions['customerId'].isin(sample_ids)]

    questions.fillna(0)
    questions['start'] = questions['contactDate'] + " " + questions['contactTimeStart']
    questions['end'] = questions['contactDate'] + " " + questions['contactTimeEnd']

    questions['log'] = 'BPIC16'
    questions.to_csv(output_path + file_name + 'Questions' + sample_string + '.csv', index=True, index_label="idx",
                     na_rep="Unknown")

    messages = pd.read_csv(os.path.realpath(input_path + 'BPI2016_Werkmap_Messages.csv'), keep_default_na=True, sep=';',
                           encoding='latin1')
    messages.columns = convert_columns_into_camel_case(messages.columns.values)
    messages = messages[messages['customerId'].isin(sample_ids)]
    messages.fillna(0)
    messages['log'] = 'BPIC16'
    messages.to_csv(output_path + file_name + 'Messages' + sample_string + '.csv', index=True, index_label="idx")


start = time.time()
create_bpic16()
end = time.time()
print("Prepared data for import in: " + str((end - start)) + " seconds.")
