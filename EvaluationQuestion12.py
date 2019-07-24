#python 3.6.5
#for use with dataset: https://data.4tu.nl/repository/uuid:5f3067df-f10b-45da-b98b-86ae4c7a310b  (converted to CSV with ProM 1.2)

#evaluation for question 19 "What are the paths between "A_Create Application" and "O_Cancelled" for those cases with at least two different offers with "O_Created" directly followed by "O_Cancelled" on the entity level"

import pandas as pd
from py2neo import Graph
import time


loan_raw = pd.read_csv('bpiChallenge17.csv', keep_default_na=False) #load full log from csv
loan_raw.drop_duplicates(keep='first', inplace=True) #remove duplicates from the dataset
loan_raw = loan_raw.reset_index(drop=True) #renew the index to close gaps of removed duplicates 

cases = loan_raw.case.unique().tolist() # create a list of all cases in the dataset
noOfCases = len(cases)

sampleIds = cases

start = time.time()  #### start csv analysis
print('Start CSV analysis...')

log = []
#for id in sampleIds:
for case in sampleIds:
    offerIds = [] # list to keep track of offerIDs within a case
    offerSequences = [] #list of activity-sequences for offers to the corresponding offerIds variable: offerIds[0] refers to offerSequences[0], etc.    
    caseSequence = [] #list  of activity-sequences for the case
    i = 0 #variable used for temp indices
    j = 0 #variable used for temp indices
    matchCount = 0 # count the "hits" - i.e. a hit = one offer where O_Created is directly followed by O_Cancelled of the same offer.
    
    for index, row in loan_raw[loan_raw.case == case].iterrows():  
        rowList = list(row) 
        if (row['EventOrigin']=="Offer" and row['event'] != "O_Create Offer"): #only execute if event is an offer event except O_Create Offer
            if (row['OfferID'] in offerIds): #if offerID has been observed before
                i = offerIds.index(row['OfferID']) #get index of that offerID
                offerSequences[i].append(row['event']) #add the activity to the sequence of the offer
            else: 
                offerIds.append(row['OfferID']) #add offerID to id list
                i = offerIds.index(row['OfferID']) #get index of that offerID
                offerSequences.append([row['event']]) #add the activity to the sequence of the offer   
        caseSequence.append(rowList) #add the complete event information (row) to case sequence list
        
    offerMatches = [False for i in range(len(offerIds))]
    for sequence in offerSequences:        
        for activity in sequence:
            if (activity == 'O_Cancelled' and len(sequence) > 1): #if current event is not the first of the sequence and activity is = O_Cancelled
                i = sequence.index(activity) #get the current activity index in the sequence list
                if (sequence[i-1] == 'O_Created'): #if the preceding activity 
                    matchCount += 1 #count as hit
                    offerMatches[j] = True #mark as match
        j += 1

            
    if(matchCount > 1):
        log.append([caseSequence,offerIds,offerSequences,offerMatches]) #add case, offerids+sequences+boolean if an offer is a "match" to new "log" if case has at least 2 hits
        

 
header = ['CaseSequence', 'OfferIds', 'OfferSequences','OfferMatches']
LogDf = pd.DataFrame(log,columns=header) #create pandas dataframe
#now we have identified all cases that meet the requirements of >= 2 offers with O_Created directly followed by O_Cancelled (with ALL their offers)

finalLog = [] #will contain a list for each offer with caseID, offerID and the sequence from start to the case till O_Cancelled of that offer
for i in LogDf.index: #for every object in the data frame
    caseId = LogDf.iloc[i]['CaseSequence'][0][0] #get caseID
    for offerId in LogDf.iloc[i]['OfferIds']: #iterate through offerID list
        index = LogDf.iloc[i]['OfferIds'].index(offerId)
        tSequence = [] #the "target" sequence per offer (A_Create Application to O_Created)
        if (LogDf.iloc[i]['OfferMatches'][index] == True): #only do for matching offers
            for activity in LogDf.iloc[i]['CaseSequence']:
                tSequence.append(activity[1]) #keep adding activities to that offers' sequence
                if (activity[1] == "O_Cancelled" and activity[11] == offerId): #until O_Cancelled of that offer is added
                    break
            finalLog.append([caseId,offerId,tSequence]) # add that offers' sequence to the final log   

header = ['CaseID', 'OfferID', 'OfferSequence']
LogDfCSV = pd.DataFrame(finalLog,columns=header) #create pandas dataframe (for stats only)          
print(str(len(LogDfCSV.CaseID.unique())) + ' Cases with ' + str(len(LogDfCSV)) + ' Offers have been identified.')                
            
            
end = time.time() #end csv analysis
    
print("The analysis of the CSV file took "+str((end - start))+" seconds to complete.")    

start = time.time() #start graph analysis
print('Start Graph analysis...')

graph = Graph(password="1234") #connect to local Neo4j DB with password

#define the query
query = """
MATCH  (e1:Event {activity: "O_Created"})<-[:O_DF]-(e2:Event {activity: "O_Cancelled"}) -[:EVENT_TO_OFFER]-> (o:Offer) -[rel:OFFER_TO_CASE]-> (c:Case)
WITH  c AS c, count(o) AS ct
WHERE ct > 1
MATCH (:Event {activity: "O_Created"})<-[:O_DF]-(e:Event {activity: "O_Cancelled"}) -[:EVENT_TO_OFFER]-> (o:Offer) -[rel:OFFER_TO_CASE]-> (c)
WITH c AS c, e AS O_Cancelled, o AS o
MATCH p = (A_Created:Event {activity: "A_Create Application"}) <-[:DF*]-(O_Cancelled:Event {activity: "O_Cancelled"}), (O_Cancelled) -[:EVENT_TO_CASE]-> (c)
RETURN p,c,o
"""


data = graph.run(query) #run query on Neo4j DB

output = pd.DataFrame([list(x) for x in data]) #save the output to pandas dataframe
finalLogGraph = [] #
for index, p in output.iterrows():
    path = []
    offerId = output[2][index]['name'] #get offerID from the current traces' last node (every trace consists of activities from A_Create Application to O_Cancelled)
    caseId = output[1][index]['name'] #get caseID, match/return of the case in the query has been included to verifify the case->offer->activity sequence 
    for node in output[0][index].nodes: #walk over the nodes
        path.append(node['activity'])#and add the activities in their order to a list
    finalLogGraph.append([caseId,offerId,path]) #add caseid,offerid and sequence to list to have the data in the same output data format as the csv analysis

header = ['CaseID', 'OfferID', 'OfferSequence']
LogDfG = pd.DataFrame(finalLogGraph,columns=header) #create pandas dataframe (for stats only)
print(str(len(LogDfG.CaseID.unique())) + ' Cases with ' + str(len(LogDfG)) + ' Offers have been identified.')    
    
end = time.time() #end Graph analysis
    
print("The analysis of the graph query took "+str((end - start))+" seconds to complete.")

print('Comparing results...')
if (sorted(finalLog) == sorted(finalLogGraph)): #check if (sorted list) results are equal
    print('The results match')
else:
    print('The results do NOT match')
