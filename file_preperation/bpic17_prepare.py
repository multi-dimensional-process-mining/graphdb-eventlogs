# loan application
import sys

import numpy as np
import pandas as pd
import time
import os

from utilities.auxiliary_functions import convert_columns_into_camel_case

# config
current_file_path = os.path.dirname(__file__)

input_path = os.path.join(current_file_path,  '..', 'data', 'BPIC17')
output_path = os.path.join(current_file_path,  '..', 'data', 'BPIC17', 'prepared')

if not os.path.isdir(output_path):
    os.makedirs(output_path)


def main():
    file_name = 'BPIC17.csv'
    start = time.time()
    create_bpi17(file_name)
    end = time.time()
    print("Prepared data for import in: " + str((end - start)) + " seconds.")


def create_bpi17(file_name: str):
    csv_log = pd.read_csv(os.path.join(input_path, 'BPI_Challenge_2017.csv'),
                          keep_default_na=True)  # load full log from csv
    csv_log.drop_duplicates(keep='first', inplace=True)  # remove duplicates from the dataset
    csv_log = csv_log.reset_index(drop=True)  # renew the index to close gaps of removed duplicates
    csv_log["log"] = 'BPIC17'
    csv_log.columns = convert_columns_into_camel_case(csv_log.columns.values)

    # "O_Create Offer": this activity belongs to an offer but has no offer ID
    # if next activity is "O_Created" (always directly follows "O_Create Offer" [verified with Disco])

    # Shift both the activity and offerId one up to have the next activity and offer Id on the same line as the current ones
    csv_log["nextActivity"] = csv_log["event"].shift(periods=-1)
    csv_log["nextOfferID"] = csv_log["offerId"].shift(periods=-1)

    # check if the current activity and next activity match the activiy names
    # if so, set offer ID to next offer ID
    # if not, keep current offer ID
    csv_log["offerId"] = np.where((csv_log["event"] == "O_Create Offer")
                                  & (csv_log["nextActivity"] == "O_Created"),
                                  csv_log["nextOfferID"],
                                  csv_log["offerId"])

    csv_log = csv_log.drop(columns=["nextActivity", "nextOfferID"])
    csv_log.to_csv(os.path.join(output_path, file_name), index=True, index_label="idx")


if __name__ == '__main__':
    main()
