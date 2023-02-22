# municipalities, building permit applications
import pandas as pd
import time

from a_scripts.additional_functions.auxiliary_functions import convert_columns_into_camel_case

## config
input_path = '../../data/BPIC15\\'
path_to_neo4j_import_directory = '../../data/BPIC15/prepared\\'  # where prepared files will be stored


def create_bpi15(path):
    for i in range(1, 6):
        file_name = f'BPIC15_{i}.csv'
        log = pd.read_csv(input_path + file_name, keep_default_na=True)
        log.columns = convert_columns_into_camel_case(log.columns.values)
        log['log'] = 'BPIC15'
        log.to_csv(path + file_name, index=True, index_label="idx")


start = time.time()
create_bpi15(path_to_neo4j_import_directory)
end = time.time()
print("Prepared data for import in: " + str((end - start)) + " seconds.")
