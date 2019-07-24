#python 3.6.5
#for use with dataset: https://data.4tu.nl/repository/uuid:5f3067df-f10b-45da-b98b-86ae4c7a310b  (converted to CSV with ProM 1.2)

import pandas as pd

loan_raw = pd.read_csv('bpiChallenge17.csv', keep_default_na=False) #load full log from csv
loan_raw.drop_duplicates(keep='first', inplace=True) #remove duplicates from the dataset
loan_raw = loan_raw.reset_index(drop=True) #renew the index to close gaps of removed duplicates 

cases = loan_raw.case.unique().tolist() # create a list of all cases in the dataset
noOfCases = len(cases)

######### uncomment for fixed cases ##########
#hard coded case IDs to ease replication
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
##############################################

#for id in sampleIds:
sampleList = [] #create a list (of lists) for the sample data containing a list of events for each of the selected cases
variants = [] # helper list for the variants
for case in sampleIds:
    offers = [] #helper list for counting the events with eventOrigin = Offer
    applications = 0 #helper variable for counting the events with eventOrigin = Application
    workflows = 0 #helper variable for counting the events with eventOrigin = workflows
    variant = [] # helper list for the variant of the current case
    for index, row in loan_raw[loan_raw.case == case].iterrows(): #first iteration through the cases for variant detection
        variant.append(row['event'])
    if not variant in variants: #if variant of the current case has not been observed before, add to the variants list
        variants.append(variant)
    for index, row in loan_raw[loan_raw.case == case].iterrows(): #second iteration through the cases for adding data     
        if row['event'] == "O_Create Offer": # this activity belongs to an offer but has no offer ID
            if loan_raw.loc[index+1]['event'] == 'O_Created':#if next activity is "O_Created" (always directly follows "O_Create Offer" [verified with Disco])
                row['OfferID'] = loan_raw.loc[index+1]['OfferID'] #assign the offerID of the next event (O_Created) to this activity
        rowList = list(row) #add the event data to rowList
        rowList.append(index) #add global index for the sequence of events
        if row['EventOrigin'] == 'Offer' and "Offer_" in row['OfferID']: #check if event has an offer ID and is an offer event
           offers.append(row['OfferID'])#add offerID entry to helper list
           rowList.extend([offers.count(row['OfferID']),0,0,"Variant_" + str(variants.index(variant)+1)]) #add order index for offer and variant
        elif (row['EventOrigin'] == 'Application'):
            applications += 1
            rowList.extend([0,applications,0,"Variant_" + str(variants.index(variant)+1)])#add order index for application and variant
        elif (row['EventOrigin'] == 'Workflow'):
            workflows += 1
            rowList.extend([0,0,workflows,"Variant_" + str(variants.index(variant)+1)])#add order index for workflow and variant
        else: # add -1 as default indices for easy detection of undesired outcomes
            rowList.extend([-1,-1,-1,"Variant_" + str(variants.index(variant)+1)])
        sampleList.append(rowList) #add the extended, single row to the sample dataset

header = list(loan_raw) #save the original header data
header.extend(['case_index','offer_index','application_index','workflow_index','variant_index']) #extend the header for the indices created for case,offer,application, workflow and variant
loan_samples = pd.DataFrame(sampleList,columns=header) #create pandas dataframe and add the samples  

# reformat time data for neo4j import 
loan_samples['startTime'] = pd.to_datetime(loan_samples['startTime'])
loan_samples['completeTime'] = pd.to_datetime(loan_samples['completeTime'])

#create separate columns (year/month/day/hour/minute/second/microsecond) for importing datetime values into neo4j as datetime object
loan_samples = loan_samples.assign(sY=loan_samples['startTime'].map(lambda x: x.year))
loan_samples = loan_samples.assign(sM=loan_samples['startTime'].map(lambda x: x.month))
loan_samples = loan_samples.assign(sD=loan_samples['startTime'].map(lambda x: x.day))
loan_samples = loan_samples.assign(sHH=loan_samples['startTime'].map(lambda x: x.hour))
loan_samples = loan_samples.assign(sMM=loan_samples['startTime'].map(lambda x: x.minute))
loan_samples = loan_samples.assign(sSS=loan_samples['startTime'].map(lambda x: x.second))
loan_samples = loan_samples.assign(sMS=loan_samples['startTime'].map(lambda x: x.microsecond))
loan_samples = loan_samples.assign(cY=loan_samples['completeTime'].map(lambda x: x.year))
loan_samples = loan_samples.assign(cM=loan_samples['completeTime'].map(lambda x: x.month))
loan_samples = loan_samples.assign(cD=loan_samples['completeTime'].map(lambda x: x.day))
loan_samples = loan_samples.assign(cHH=loan_samples['completeTime'].map(lambda x: x.hour))
loan_samples = loan_samples.assign(cMM=loan_samples['completeTime'].map(lambda x: x.minute))
loan_samples = loan_samples.assign(cSS=loan_samples['completeTime'].map(lambda x: x.second))
loan_samples = loan_samples.assign(cMS=loan_samples['completeTime'].map(lambda x: x.microsecond))


if not (any(loan_samples.offer_index == -1) or any(loan_samples.application_index == -1) or any(loan_samples.workflow_index == -1)):   #check if any event has an invalid index
    #write to file 
    sampleFile = 'loan_sample.csv' 
    loan_samples.to_csv(sampleFile, index=False)
    print('Data preparation successful - data written to file for Neo4j import')
else:
    print('NO FILE WRITTEN - DATAPREP NOT SUCCESSFUL')


show=loan_samples[['case','event','OfferID','case_index','offer_index','application_index','workflow_index','variant_index']]
uniqueOffers = loan_samples.OfferID.unique().tolist() # create a list of all offers in the sampleset


wfEvents = loan_samples[loan_samples['EventOrigin'] == 'Workflow']
appEvents = loan_samples[loan_samples['EventOrigin'] == 'Application']
offEvents = loan_samples[loan_samples['EventOrigin'] == 'Offer']