import pandas as pd
import time, os

from utilities.auxiliary_functions import convert_columns_into_camel_case

### config
current_file_path = os.path.dirname(__file__)

input_path = os.path.join(current_file_path,  '..', 'data', 'BPIC19')
# where prepared files will be stored
output_path = os.path.join(current_file_path,  '..', 'data', 'BPIC19', 'prepared')
if not os.path.isdir(output_path):
    os.makedirs(output_path)


def create_bpi19():
    csv_log = pd.read_csv(os.path.join(input_path, 'BPI_Challenge_2019.csv'),
                          keep_default_na=True, encoding='cp1252')  # load full log from csv
    csv_log["log"] = 'BPIC19'
    csv_log.columns = convert_columns_into_camel_case(csv_log.columns.values)

    csv_log.to_csv(os.path.join(output_path, 'BPIC19.csv'), index=True, index_label="idx")


if __name__ == "__main__":
    start = time.time()
    create_bpi19()
    end = time.time()
    print("Prepared data for import in: " + str((end - start)) + " seconds.")
