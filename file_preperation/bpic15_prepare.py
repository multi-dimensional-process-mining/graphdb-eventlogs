# municipalities, building permit applications
import os

import pandas as pd
import time

from utilities.auxiliary_functions import convert_columns_into_camel_case

## config
current_file_path = os.path.dirname(__file__)

input_path = os.path.join(current_file_path,  '..', 'data', 'BPIC15')
output_path = os.path.join(current_file_path,  '..', 'data', 'BPIC15', 'prepared')
if not os.path.isdir(output_path):
    os.makedirs(output_path)


def create_bpi15():
    for i in range(1, 6):
        file_name = f'BPIC15_{i}.csv'
        log = pd.read_csv(os.path.join(input_path, file_name), keep_default_na=True)
        log.columns = convert_columns_into_camel_case(log.columns.values)
        log['log'] = 'BPIC15'
        log.to_csv(os.path.join(output_path, file_name), index=True, index_label="idx")


if __name__ == "__main__":
    start = time.time()
    create_bpi15()
    end = time.time()
    print("Prepared data for import in: " + str((end - start)) + " seconds.")
