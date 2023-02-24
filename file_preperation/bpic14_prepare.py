# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 14:02:29 2019

@author: 20175070
"""
import os
import sys

import numpy as np
import pandas as pd
import time

from utilities.auxiliary_functions import convert_columns_into_camel_case


def main():
    # config
    current_file_path = os.path.dirname(__file__)

    input_path = os.path.join(current_file_path,  '..', 'data', 'BPIC14')
    output_path = os.path.join(current_file_path,  '..', 'data', 'BPIC14', 'prepared')
    if not os.path.isdir(output_path):
        os.makedirs(output_path)

    start = time.time()

    change = pd.read_csv(os.path.join(input_path, 'Detail_Change.csv'), keep_default_na=True, sep=';')
    change.columns = convert_columns_into_camel_case(change.columns.values)
    change['log'] = 'BPIC14'
    change = change.reset_index(drop=True)
    change.to_csv(os.path.join(output_path, 'BPIC14Change.csv'), index=True, index_label="idx")

    incident = pd.read_csv(os.path.join(input_path, 'Detail_Incident.csv'), keep_default_na=True, sep=';', decimal=",",
                           dtype={"Urgency": "str"})
    incident.columns = convert_columns_into_camel_case(incident.columns.values)
    # only keep numeric values for urgency column and convert to Int64
    incident["urgency"] = incident["urgency"].str.replace('(\D+)', '', regex=True)
    incident["urgency"] = incident["urgency"].astype('Int64')
    incident['log'] = 'BPIC14'
    incident = incident.dropna(how='all', axis=1)  # drop all columns in which all values are nan (empty)
    incident = incident.dropna(thresh=19)  # drops all 'nan-only' rows
    incident = incident.reset_index(drop=True)
    incident.to_csv(os.path.join(output_path, "BPIC14Incident.csv"),
                    index=True, index_label="idx")

    incident_detail = pd.read_csv(os.path.join(input_path, 'Detail_Incident_Activity.csv'), keep_default_na=True,
                                  sep=';')
    incident_detail.columns = convert_columns_into_camel_case(incident_detail.columns.values)
    incident_detail['log'] = 'BPIC14'
    incident_detail = incident_detail.reset_index(drop=True)
    incident_detail.to_csv(os.path.join(output_path, 'BPIC14IncidentDetail.csv'), index=True, index_label="idx")

    interaction = pd.read_csv(os.path.join(input_path, 'Detail_Interaction.csv'), keep_default_na=True, sep=';',
                              dtype={"Urgency": "str"})
    interaction.columns = convert_columns_into_camel_case(interaction.columns.values)
    # only keep numeric values for urgency column and convert to Int64
    interaction["urgency"] = interaction["urgency"].str.replace('(\D+)', '', regex=True)
    interaction["urgency"] = interaction["urgency"].astype('Int64')
    interaction['log'] = 'BPIC14'
    interaction = interaction.reset_index(drop=True)
    interaction.to_csv(os.path.join(output_path, "BPIC14Interaction.csv"), index=True, index_label="idx")

    end = time.time()
    print("Prepared data for import in: " + str((end - start)) + " seconds.")


if __name__ == "__main__":
    main()
