# municipalities, building permit applications
import pandas as pd
import time

from a_scripts.additional_functions.auxiliary_functions import convert_columns_into_camel_case

## config
input_path = '../../data/BoxProcess\\'
path_to_neo4j_import_directory = '../../data/BoxProcess/prepared\\'  # where prepared files will be stored


def create_boxprocess(path):
    file_name = f'data.csv'
    log = pd.read_csv(input_path + file_name, keep_default_na=True, sep=";", dtype={"Equipment": "Int64"})
    log.columns = convert_columns_into_camel_case(log.columns.values)
    log['log'] = 'Running Example'
    log.to_csv(path + file_name, index=True, index_label="idx")


start = time.time()
create_boxprocess(path_to_neo4j_import_directory)
end = time.time()
print("Prepared data for import in: " + str((end - start)) + " seconds.")
