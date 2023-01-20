# municipalities, building permit applications
import pandas as pd
import time, os, csv

from csv_to_eventgraph_neo4j.auxiliary_functions import convert_columns_into_camel_case

## config
sample = False
inputpath = '..\\data\\\\BPIC15\\'
path_to_neo4j_import_directory = '..\\data\\\\BPIC15\\prepared\\'  # where prepared files will be stored


def CreateBPI15(path, sample=False):
    if (sample):
        sampleIDs = []
        sampleIDs.append(
            [9691153, 9294701, 3084709, 4375473, 3837125, 4397208, 5843747, 3606967, 11773351, 8051084, 8021700,
             2847199, 8958665, 2794023, 2899336, 8816378, 5288872, 5106291, 10997930, 6985928])
        sampleIDs.append(
            [20284125, 23142529, 3585731, 21038392, 19995133, 3874234, 22139866, 4623044, 3948666, 3702964, 20930456,
             20063170, 3808540, 20025552, 20888077, 21986652, 20235405, 19940265, 4395219, 20742337])
        sampleIDs.append(
            [3055383, 6616348, 3963963, 5245298, 5007761, 6015049, 3824284, 5489846, 3691884, 3965191, 7313344, 3871869,
             5718151, 3721360, 5585560, 5702384, 5562464, 5961864, 3829589, 4350022])
        sampleIDs.append(
            [10516319, 9279433, 4235583, 5932529, 8577457, 6873521, 7084443, 4968228, 5421204, 4578191, 8352642,
             4800472, 11167513, 9665535, 7345969, 9095800, 7533366, 5673981, 5902757, 11164523])
        sampleIDs.append(
            [10873742, 4171558, 4495115, 10837828, 3634775, 8126523, 3589696, 10286510, 7888926, 4532439, 6365048,
             8460573, 8315159, 6793246, 6791482, 8612436, 4927985, 4637475, 4592223, 8114679])

    def convert_dtype(x):
        if not x:
            return ''
        try:
            return str(x)
        except:
            return ''

    for i in range(1, 6):
        fileName = f'BPIC15_{i}.csv'
        log = pd.read_csv(inputpath + fileName, keep_default_na=True, converters={'dateStop': convert_dtype})
        log.columns = convert_columns_into_camel_case(log.columns.values)
        log['log'] = 'BPIC15'

        if i == 2:
            log = log.drop(columns=['actionCode', 'activityNameNl', 'caseType'])
        else:
            log = log.drop(columns=['actionCode', 'endDatePlanned', 'activityNameNl', 'caseType'])

        if (sample):
            log = log[log['case'].isin(sampleIDs[i - 1])]

        log['idofConceptCase'] = log['idofConceptCase'].astype('Int64', errors='ignore')
        log['idofConceptCase'] = log['idofConceptCase'].fillna(-1)
        log['responsibleActor'] = log['responsibleActor'].astype('Int64', errors='ignore')
        log['responsibleActor'] = log['responsibleActor'].fillna(-1)
        log['landRegisterId'] = log['landRegisterId'].astype('Int64', errors='ignore')
        log['landRegisterId'] = log['landRegisterId'].fillna(-1)

        log['startTime'] = pd.to_datetime(log['startTime'], format='%Y/%m/%d %H:%M:%S.%f')
        log['startTime'] = log['startTime'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S%z'))
        log['completeTime'] = pd.to_datetime(log['completeTime'], format='%Y/%m/%d %H:%M:%S.%f')
        log['completeTime'] = log['completeTime'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S%z'))

        if (sample):
            log.to_csv(path + fileName[0:-4] + '_sample.csv', index=True, index_label="idx", na_rep="Unknown")
        else:
            log.to_csv(path + fileName, index=True, index_label="idx", na_rep="Unknown")


start = time.time()
CreateBPI15(path_to_neo4j_import_directory, sample)
end = time.time()
print("Prepared data for import in: " + str((end - start)) + " seconds.")
