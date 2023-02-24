# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 16:51:36 2019

@author: 20175070
"""

# website click data, labour services process
import pandas as pd
import time, os

from utilities.auxiliary_functions import convert_columns_into_camel_case

# config
current_file_path = os.path.dirname(__file__)
input_path = os.path.join(current_file_path,  '..', 'data', 'BPIC16')
output_path = os.path.join(current_file_path,  '..', 'data', 'BPIC16', 'prepared')  # where prepared files will be stored

if not os.path.isdir(output_path):
    os.makedirs(output_path)

file_name = 'BPIC16'


################## data prep ##################

def create_bpic16():
    clicks_log = pd.read_csv(os.path.join(input_path, 'BPI2016_Clicks_Logged_In.csv'), keep_default_na=True,
                             sep=';', encoding='latin1')
    clicks_log.columns = convert_columns_into_camel_case(clicks_log.columns.values)
    ids = clicks_log['customerId'].unique().tolist()
    clicks_log = clicks_log[clicks_log['customerId'].isin(ids)]
    clicks_log['log'] = 'BPIC16'
    clicks_log['timestamp'] = clicks_log['timestamp'].apply(lambda x: f'{x}.000' if '.' not in x else x)
    clicks_log.to_csv(os.path.join(output_path, 'BPIC16Clicks.csv'), index=True, index_label="idx")

    complaints = pd.read_csv(os.path.join(input_path, 'BPI2016_Complaints.csv'), keep_default_na=True, sep=';',
                             encoding='latin1')
    complaints.columns = convert_columns_into_camel_case(complaints.columns.values)
    complaints = complaints[complaints['customerId'].isin(ids)]
    complaints['log'] = 'BPIC16'
    complaints.to_csv(os.path.join(output_path, 'BPIC16Complaints.csv'), index=True, index_label="idx",
                      na_rep="Unknown")

    questions = pd.read_csv(os.path.join(input_path, 'BPI2016_Questions.csv'), keep_default_na=True, sep=';',
                            encoding='latin1')
    questions.columns = convert_columns_into_camel_case(questions.columns.values)
    questions = questions[questions['customerId'].isin(ids)]
    questions['log'] = 'BPIC16'
    questions.to_csv(os.path.join(output_path, 'BPIC16Questions.csv'), index=True, index_label="idx",
                     na_rep="Unknown")

    messages = pd.read_csv(os.path.join(input_path, 'BPI2016_Werkmap_Messages.csv'), keep_default_na=True, sep=';',
                           encoding='latin1')
    messages.columns = convert_columns_into_camel_case(messages.columns.values)
    messages = messages[messages['customerId'].isin(ids)]
    messages['log'] = 'BPIC16'
    messages.to_csv(os.path.join(output_path, 'BPIC16Messages.csv'), index=True, index_label="idx")


if __name__ == "__main__":
    start = time.time()
    create_bpic16()
    end = time.time()
    print("Prepared data for import in: " + str((end - start)) + " seconds.")
