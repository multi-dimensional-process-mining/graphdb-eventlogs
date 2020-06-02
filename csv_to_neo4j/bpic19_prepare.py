import pandas as pd
import time, csv, os

### config

sample=False
inputpath = '.\\BPIC19\\'
path_to_neo4j_import_directory = 'C:\\Temp\\Import\\' # where prepared files will be stored



def CreateBPI19(inputpath, path_to_neo4j_import_directory, fileName, bSample):
    datasetList = []
    headerCSV = []
    i = 0
    print('Loading source ' + str(time.time()))
    with open(os.path.realpath(inputpath+'BPI_Challenge_2019.csv')) as f:
        reader = csv.reader(f)
        for row in reader:
            if (i==0):
                headerCSV = list(row)
                i +=1
            else:
               datasetList.append(row)
    print('Renaming columns ' + str(time.time()))        
    csvLog = pd.DataFrame(datasetList,columns=headerCSV)
       
    csvLog.drop(columns=['event User','case Source'], inplace=True) #redundant
    
    csvLog.rename(columns={'case concept:name':'cID',
                           'case Purchasing Document':'cPOID',
                           'eventID ':'ID',
                           'case Spend area text':'cSpendAreaText',
                           'case Company':'cCompany',
                           'case Document Type':'cDocType',
                           'case Sub spend area text':'cSubSPendAreaText',
                           'case Purch. Doc. Category name':'cPurDocCat',
                           'case Vendor':'cVendor',
                           'case Item Type':'cItemType',
                           'case Item Category':'cItemCat',
                           'case Spend classification text':'cSpendClassText',
                           'case Name':'cVendorName',
                           'case GR-Based Inv. Verif.':'cGRbasedInvVerif',
                           'case Item':'cItem',
                           'case Goods Receipt':'cGR',
                           'event org:resource':'resource',
                           'event concept:name':'Activity',
                           'event Cumulative net worth (EUR)':'eCumNetWorth',
                           'event time:timestamp':'timestamp'}, inplace=True)
    print('Changing DateTime format '+ str(time.time()))
    csvLog['timestamp'] = pd.to_datetime(csvLog['timestamp'], format='%d-%m-%Y %H:%M:%S.%f')
    csvLog['timestamp'] = csvLog['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S.%f')[0:-3]+'+0100')


    if (bSample == True): 
        sampleIds = ['4508062571',
         '4507010217',
         '4507000321',
         '4507040910',  
         '4507021063',
         '4507024440',
         '4507001109',
         '4507020425',
         '4507014406',
         '4507018608',
         '4508066411',
         '4508053414',
         '4507010940',
         '4507022053',
         '4507016146',
         '4508044395',
         '4508072550',
         '4507002104',
         '4507020767',
         '4508057849']
        
        dfSize = len(csvLog.index)
        #PO is defined as case (instead of PO line item)
        sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
        i = 0
        for case in sampleIds:
            for index, row in csvLog[csvLog.cPOID == case].iterrows():
                i += 1
                rowList = list(row) #add the event data to rowList
                sampleList.append(rowList) #add the extended, single row to the sample dataset
            if (i%100 == 0):
                print(i+' of '+dfSize+' written...')
        
        header =  list(csvLog) #save the updated header data
        logSamples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples 
        logSamples.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")
         
    else:
        csvLog.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")




if(sample):
    fileName = 'BPIC19sample.csv' 
    perfFileName = 'BPIC19samplePerformance.csv'
else:
    fileName = 'BPIC19full.csv'
    perfFileName = 'BPIC19fullPerformance.csv'


start = time.time()
CreateBPI19(inputpath, path_to_neo4j_import_directory,fileName,sample)
end = time.time()
print("Prepared data for import in: "+str((end - start))+" seconds.") 



