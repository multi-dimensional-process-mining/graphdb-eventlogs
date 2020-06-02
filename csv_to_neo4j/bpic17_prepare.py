#loan application

import pandas as pd
import time, os, csv

#config

sample = False
inputpath = '.\\BPIC17\\'
path_to_neo4j_import_directory = 'C:\\Temp\\Import\\'

def LoadLog(localFile):
    datasetList = []
    headerCSV = []
    i = 0
    with open(localFile) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i==0):
                headerCSV = list(row)
                i +=1
            else:
               datasetList.append(row)
        
    log = pd.DataFrame(datasetList,columns=headerCSV)
    
    return headerCSV, log

def CreateBPI17(inputpath, path_to_neo4j_import_directory, fileName, sample):
    csvLog = pd.read_csv(os.path.realpath(inputpath+'BPI_Challenge_2017.csv'), keep_default_na=True) #load full log from csv                  
    csvLog.drop_duplicates(keep='first', inplace=True) #remove duplicates from the dataset
    csvLog = csvLog.reset_index(drop=True) #renew the index to close gaps of removed duplicates 
    
    
    if (sample == True): 
        sampleIds = ['Application_2045572635', 
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
        sampleIds = [] #csvLog.case.unique().tolist() # create a list of all cases in the dataset
    
    # rename CSV columns to standard value
    # Activity
    # timestamp
    # resource
    # lifecycle for life-cycle transtiion
    csvLog = csvLog.rename(columns={'event': 'Activity','time':'timestamp','org:resource':'resource','lifecycle:transition':'lifecycle'})
    csvLog['EventIDraw'] = csvLog['EventID']

    sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
    # fix missing entity identifier for one record: check all records in the list of sample cases (or the entire dataset)
    for index, row in csvLog.iterrows():
        if sampleIds == [] or row['case'] in sampleIds:
            if row['Activity'] == "O_Create Offer": # this activity belongs to an offer but has no offer ID
                if csvLog.loc[index+1]['Activity'] == 'O_Created':#if next activity is "O_Created" (always directly follows "O_Create Offer" [verified with Disco])
                    row['OfferID'] = csvLog.loc[index+1]['OfferID'] #assign the offerID of the next event (O_Created) to this activity
            rowList = list(row) #add the event data to rowList
            sampleList.append(rowList) #add the extended, single row to the sample dataset
    
    header =  list(csvLog) #save the updated header data
    logSamples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples  

    logSamples['timestamp'] = pd.to_datetime(logSamples['timestamp'], format='%Y/%m/%d %H:%M:%S.%f')
    
    logSamples.fillna(0)
    logSamples.sort_values(['case','timestamp'], inplace=True)
    logSamples['timestamp'] = logSamples['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3]+'+0100')
    
    logSamples.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")


if(sample):
    fileName = 'BPIC17sample.csv' 
    perfFileName = 'BPIC17samplePerformance.csv'
else:
    fileName = 'BPIC17full.csv'
    perfFileName = 'BPIC17fullPerformance.csv'
    

start = time.time()
CreateBPI17(inputpath, path_to_neo4j_import_directory, fileName, sample)
end = time.time()
print("Prepared data for import in: "+str((end - start))+" seconds.") 
