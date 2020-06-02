# -*- coding: utf-8 -*-
"""
Created on Mon Jun 17 16:51:36 2019

@author: 20175070
"""

#website click data, labour services process
import pandas as pd
import  os


#config
sample=False
inputpath = '.\\BPIC16\\'
path_to_neo4j_import_directory = 'C:\\Temp\\Import\\' # where prepared files will be stored


################## data prep ##################  
    
def CreateBPI16(inputpath, outputpath, fileName, sample):
    
    clicksLog = pd.read_csv(os.path.realpath(inputpath+'BPI2016_Clicks_Logged_In.csv'), keep_default_na=True, sep=';',encoding='latin1')
    complaints = pd.read_csv(os.path.realpath(inputpath+'BPI2016_Complaints.csv'), keep_default_na=True, sep=';',encoding='latin1')
    questions = pd.read_csv(os.path.realpath(inputpath+'BPI2016_Questions.csv'), keep_default_na=True, sep=';',encoding='latin1')
    messages = pd.read_csv(os.path.realpath(inputpath+'BPI2016_Werkmap_Messages.csv'), keep_default_na=True, sep=';',encoding='latin1')
    
    if (sample): 
        sampleIds = [2026796, 2223803, 2023026, 114939, 2011721, 2022933, 919259, 2079086, 466152, 2057965, 1039204, 395673, 1710155, 2081135, 1723340, 1893155, 1042998, 435939, 1735039, 2045407]
    else:
        sampleIds = clicksLog.CustomerID.unique().tolist() # create a list of all cases in the dataset
        
    csvLog = complaints
    fileNameTmp = fileName[0:-4]+'Complaints.csv' 
    sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
    for case in sampleIds:
        for index, row in csvLog[csvLog.CustomerID == case].iterrows(): #second iteration through the cases for adding data
            rowList = list(row) #add the event data to rowList
            sampleList.append(rowList) #add the extended, single row to the sample dataset
    
    header =  list(csvLog) #save the updated header data
    logSamples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples  
    logSamples.fillna(0)
    logSamples['ContactDate'] = pd.to_datetime(logSamples['ContactDate'], format='%Y-%m-%d')
    logSamples['ContactDate'] = logSamples['ContactDate'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3]+'+0100')
    logSamples = logSamples.rename(columns={'ContactDate': 'timestamp','ComplaintTheme': 'Activity'})

    logSamples.to_csv(outputpath+fileNameTmp, index=True, index_label="idx",na_rep="Unknown")
    logSamples['idx'] = logSamples.index


    complaints = logSamples
    
    csvLog = questions
    fileNameTmp = fileName[0:-4]+'Questions.csv'
    sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
    for case in sampleIds:
        for index, row in csvLog[csvLog.CustomerID == case].iterrows(): #second iteration through the cases for adding data
            rowList = list(row) #add the event data to rowList
            sampleList.append(rowList) #add the extended, single row to the sample dataset
    
    header =  list(csvLog) #save the updated header data
    logSamples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples  
    logSamples.fillna(0)
    logSamples['ContactDate'] = pd.to_datetime(logSamples['ContactDate'], format='%Y-%m-%d')
    logSamples['ContactDate'] = logSamples['ContactDate'].map(lambda x: x.strftime('%Y-%m-%d'))
    logSamples['ContactTimeStart'] = pd.to_datetime(logSamples['ContactTimeStart'], format='%H:%M:%S.%f')
    logSamples['ContactTimeStart'] = logSamples['ContactTimeStart'].map(lambda x: x.strftime('%H:%M:%S.%f')[0:-3])
    logSamples['start'] = logSamples['ContactDate']+"T"+  logSamples['ContactTimeStart'] +'+0100'
    logSamples['ContactTimeEnd'] = pd.to_datetime(logSamples['ContactTimeEnd'], format='%H:%M:%S.%f')
    logSamples['ContactTimeEnd'] = logSamples['ContactTimeEnd'].map(lambda x: x.strftime('%H:%M:%S.%f')[0:-3])
    logSamples['end'] = logSamples['ContactDate']+"T"+  logSamples['ContactTimeEnd'] +'+0100'
    logSamples = logSamples.rename(columns={'end': 'timestamp','QuestionTheme': 'Activity'})

    logSamples.to_csv(outputpath+fileNameTmp, index=True, index_label="idx",na_rep="Unknown")
    logSamples['idx'] = logSamples.index

    
    questions = logSamples


    
    csvLog = messages
    fileNameTmp = fileName[0:-4]+'Messages.csv'
    sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
    for case in sampleIds:
        for index, row in csvLog[csvLog.CustomerID == case].iterrows(): #second iteration through the cases for adding data
            rowList = list(row) #add the event data to rowList
            sampleList.append(rowList) #add the extended, single row to the sample dataset
    
    header =  list(csvLog) #save the updated header data
    logSamples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples  
    logSamples.fillna(0)
    logSamples['EventDateTime'] = pd.to_datetime(logSamples['EventDateTime'], format='%Y-%m-%d %H:%M:%S.%f')
    logSamples['EventDateTime'] = logSamples['EventDateTime'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3]+'+0100')
    logSamples = logSamples.rename(columns={'EventDateTime': 'timestamp','EventType': 'Activity'})
    # logSamples['idx'] = range(1, len(logSamples) + 1)
    
    # logSamples['MessageID'] = logSamples['idx'].astype(str) #add prefix to entity ids
    
    logSamples.to_csv(outputpath+fileNameTmp, index=True, index_label="idx")
    
    messages = logSamples
 
    
    
    csvLog = clicksLog
    fileNameTmp = fileName[0:-4]+'Clicks.csv'
    sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
    for case in sampleIds:
        for index, row in csvLog[csvLog.CustomerID == case].iterrows(): #second iteration through the cases for adding data
            rowList = list(row) #add the event data to rowList
            sampleList.append(rowList) #add the extended, single row to the sample dataset
    
    header =  list(csvLog) #save the updated header data
    logSamples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples  
    logSamples.fillna(0)
    logSamples['TIMESTAMP'] = pd.to_datetime(logSamples['TIMESTAMP'], format='%Y-%m-%d %H:%M:%S.%f')
    logSamples['TIMESTAMP'] = logSamples['TIMESTAMP'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3]+'+0100')
    logSamples = logSamples.rename(columns={'TIMESTAMP': 'timestamp','PAGE_NAME': 'Activity'})
    logSamples = logSamples.drop(logSamples.columns[[range(-1,-10,-1)]], axis=1)
    

    logSamples.to_csv(outputpath+fileNameTmp, index=True, index_label="idx")
    logSamples['idx'] = logSamples.index

    
    clicksLog = logSamples
    
    return clicksLog, complaints, questions, messages



if(sample):
    fileName = 'BPIC16sample.csv'
else:
    fileName = 'BPIC16full.csv'

clicksLog, complaints, questions, messages = CreateBPI16(inputpath,path_to_neo4j_import_directory,fileName,sample)