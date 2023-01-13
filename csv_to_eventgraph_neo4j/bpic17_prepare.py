# loan application
import sys

import numpy as np
import pandas as pd
import time
import os
import csv

# config
is_sample = True
input_path = '..\\data\\BPIC17\\'
path_to_neo4j_import_directory = '..\\data\\BPIC17\\prepared\\'


def main():
    if is_sample:
        file_name = 'BPIC17sample.csv'
        perf_file_name = 'BPIC17samplePerformance.csv'
    else:
        file_name = 'BPIC17full.csv'
        perf_file_name = 'BPIC17fullPerformance.csv'

    start = time.time()
    create_bpi17(file_name)
    end = time.time()
    print("Prepared data for import in: " + str((end - start)) + " seconds.")


def create_bpi17(file_name: str):
    csv_log = pd.read_csv(os.path.realpath(input_path + 'BPI_Challenge_2017.csv'),
                          keep_default_na=True)  # load full log from csv
    csv_log.drop_duplicates(keep='first', inplace=True)  # remove duplicates from the dataset
    csv_log = csv_log.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    csv_log["Log"] = 'BPIC17'

    if is_sample:
        sample_ids = ['Application_2045572635',
                      'Application_2014483796',
                      'Application_1973871032',
                      'Application_1389621581',
                      'Application_1564472847',
                      'Application_430577010',
                      'Application_889180637',
                      'Application_1065734594',
                      'Application_681547497',
                      'Application_1020381296',
                      'Application_180427873',
                      'Application_2103964126',
                      'Application_55972649',
                      'Application_1076724533',
                      'Application_1639247005',
                      'Application_1465025013',
                      'Application_1244956957',
                      'Application_1974117177',
                      'Application_797323371',
                      'Application_1631297810']
    else:
        sample_ids = []  # csv_log.case.unique().tolist() # create a list of all cases in the dataset

    # rename CSV columns to standard value
    # Activity
    # timestamp
    # resource
    # lifecycle for life-cycle transition
    csv_log = csv_log.rename(columns={'event': 'Activity', 'time': 'timestamp', 'org:resource': 'resource',
                                      'lifecycle:transition': 'lifecycle'})
    csv_log['EventIDraw'] = csv_log['EventID']

    # fix missing entity identifier for one record: check all records in the list of sample cases
    # (or the entire dataset)

    # "O_Create Offer": this activity belongs to an offer but has no offer ID
    # if next activity is "O_Created" (always directly follows "O_Create Offer" [verified with Disco])

    # Shift both the activity and offerID one up to have the next activity and offer Id on the same line as the current ones
    csv_log["Next_Activity"] = csv_log["Activity"].shift(periods=-1)
    csv_log["Next_OfferID"] = csv_log["OfferID"].shift(periods=-1)

    # check if the current activity and next activity match the activiy names
    # if so, set offer ID to next offer ID
    # if not, keep current offer ID
    csv_log["OfferID"] = np.where((csv_log["Activity"] == "O_Create Offer")
                                  & (csv_log["Next_Activity"] == "O_Created"),
                                  csv_log["Next_OfferID"],
                                  csv_log["OfferID"])

    csv_log = csv_log.drop(columns=["Next_Activity", "Next_OfferID"])

    if sample_ids:  # if there are values in sample ids, then we only take these cases
        csv_log = csv_log[csv_log['case'].isin(sample_ids)]

    csv_log['timestamp_dt'] = pd.to_datetime(csv_log['timestamp'], format='%Y/%m/%d %H:%M:%S.%f')

    csv_log.fillna(0)
    csv_log.sort_values(['case', 'timestamp_dt'], inplace=True)

    # reformat in correct timestamp
    csv_log['timestamp'] = csv_log['timestamp'].replace([' ', '/'], ['T', '-'], regex=True) + '+0100'
    csv_log.drop(columns=["timestamp_dt"], inplace=True)

    csv_log.to_csv(path_to_neo4j_import_directory + file_name, index=True, index_label="ID", na_rep="Unknown")


if __name__ == '__main__':
    sys.exit(main())
