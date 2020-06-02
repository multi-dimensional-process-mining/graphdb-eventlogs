# -*- coding: utf-8 -*-
"""
Created on Mon Jul  1 14:02:29 2019

@author: 20175070
"""
#incidents
import pandas as pd
import random

#config
sample = False
inputpath = '.\\BPIC14\\'
path_to_neo4j_import_directory = 'C:\\Temp\\Import\\' # where prepared files will be stored




### data prep

change = pd.read_csv(inputpath+f'Detail_Change.csv', keep_default_na=True, sep=';')
incident = pd.read_csv(inputpath+f'Detail_Incident.csv', keep_default_na=True, sep=';')
incidentDetail = pd.read_csv(inputpath+f'Detail_Incident_Activity.csv', keep_default_na=True, sep=';')
interaction = pd.read_csv(inputpath+f'Detail_Interaction.csv', keep_default_na=True, sep=';')



incident.drop(incident.iloc[:, 28:78], inplace=True, axis=1) #drop all empty columns
incident = incident.dropna(thresh=19) #drops all 'nan-only' rows


change.rename(columns={'Service Component WBS (aff)':'ServiceComponentAff',#sample by
                       'CI Name (aff)':'CINameAff',
                       'CI Type (aff)':'CITypeAff',
                       'CI Subtype (aff)':'CISubTypeAff',
                       'Change ID':'ChangeID',
                       'Change Type':'Activity',
                       'Risk Assessment':'RiskAssessment',
                       'Emergency Change':'EmergencyChange',
                       'CAB-approval needed':'CABApprovalNeeded',
                       'Planned Start':'PlannedStart',
                       'Planned End':'PlannedEnd',
                       'Scheduled Downtime Start':'ScheduledDowntimeStart',
                       'Scheduled Downtime End':'ScheduledDowntimeEnd',
                       'Actual Start':'ActualStart',
                       'Actual End':'ActualEnd',
                       'Requested End Date':'RequestedEndDate',
                       'Change record Open Time':'start',#only 2 timestamps with no null values
                       'Change record Close Time':'timestamp',#only 2 timestamps with no null values
                       'Originated from':'OriginatedFrom',
                       '# Related Interactions':'NoRelatedInteractions',
                       '# Related Incidents':'NoRelatedIncidents'
                       }, inplace=True)




incident.rename(columns={'Service Component WBS (aff)':'ServiceComponentAff',#sample by
                         'CI Name (aff)':'CINameAff',
                         'CI Type (aff)':'CITypeAff',
                         'CI Subtype (aff)':'Activity',
                         'Incident ID':'IncidentID',
                         'KM number':'KMNo',
                         'Alert Status':'AlertStatus',
                         '# Reassignments':'NoReassignments',
                         'Open Time':'start', #only 2 timestamps with no null values
                         'Reopen Time':'ReopenTime',
                         'Resolved Time':'ResolvedTime',
                         'Close Time':'timestamp', #only 2 timestamps with no null values
                         'Handle Time (Hours)':'HandleTime',
                         'Closure Code':'ClosureCode',
                         '# Related Interactions':'NoRelatedInteractions',
                         'Related Interaction':'RelatedInteraction',
                         '# Related Incidents':'NoRelatedIncidents',
                         '# Related Changes':'NoRelatedChanges',
                         'Related Change':'RelatedChange',
                         'CI Name (CBy)':'CINameCBy',
                         'CI Type (CBy)':'CITypeCBy',
                         'CI Subtype (CBy)':'CISubTypeCBy',
                         'ServiceComp WBS (CBy)':'ServiceComponentCBy'}, inplace=True)


incidentDetail.rename(columns={'Incident ID':'IncidentID',#sample by
                               'DateStamp':'timestamp', #timestamp
                               'IncidentActivity_Number':'IncidentActivityNumber',
                               'IncidentActivity_Type':'Activity',
                               'Assignment Group':'AssignmentGroup',
                               'KM number':'KMNo',
                               'Interaction ID':'InteractionID'}, inplace=True)

interaction.rename(columns={'Service Comp WBS (aff)':'ServiceComponentAff',#sample by
                            'CI Name (aff)':'CINameAff',
                            'CI Type (aff)':'CITypeAff',
                            'CI Subtype (aff)':'CISubTypeAff',
                            'Interaction ID':'InteractionID',
                            'KM number':'KMNo',
                            'Open Time (First Touch)':'start', #start timestamp
                            'Close Time':'timestamp', #end timestamp
                            'Closure Code':'ClosureCode',
                            'First Call Resolution':'FirstCallResolution',
                            'Handle Time (secs)':'HandleTime',
                            'Related Incident':'RelatedIncident',
                            'Category':'Activity'}, inplace=True)
    
    
    
    
if sample: ## global sample params
    random.seed(1)
    change = change[change['ServiceComponentAff'].isin(random.sample(change.ServiceComponentAff.unique().tolist(),20))]
    incident = incident[incident['ServiceComponentAff'].isin(random.sample(incident.ServiceComponentAff.unique().tolist(),20))]
    incidentDetail = incidentDetail[incidentDetail['IncidentID'].isin(random.sample(incidentDetail.IncidentID.unique().tolist(),20))]
    interaction = interaction[interaction['ServiceComponentAff'].isin(random.sample(interaction.ServiceComponentAff.unique().tolist(),10))]






change['start'] = pd.to_datetime(change['start'], format='%d-%m-%Y %H:%M')
change['start'] = change['start'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M')+':00.000+0100')
change['timestamp'] = pd.to_datetime(change['timestamp'], format='%d-%m-%Y %H:%M')
change['timestamp'] = change['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M')+':00.000+0100')
change = change.reset_index(drop=True)

if sample: #sample params for log file
    fileName = 'BPIC14Change_sample.csv'
else:
    fileName = 'BPIC14Change.csv'
 
change.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")



incident = incident.replace(['#MULTIVALUE','#N/B'], 'Unknown')
incident['start'] = pd.to_datetime(incident['start'], format='%d/%m/%Y %H:%M:%S')
incident['start'] = incident['start'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S')+'.000+0100')
incident['timestamp'] = pd.to_datetime(incident['timestamp'], format='%d/%m/%Y %H:%M:%S')
incident['timestamp'] = incident['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S')+'.000+0100')
incident = incident.reset_index(drop=True)
if sample: #sample params for log file
    fileName = 'BPIC14Incident_sample.csv'
else:
    fileName = 'BPIC14Incident.csv'

incident.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")



incidentDetail['timestamp'] = pd.to_datetime(incidentDetail['timestamp'], format='%d-%m-%Y %H:%M:%S')
incidentDetail['timestamp'] = incidentDetail['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S')+'.000+0100')
incidentDetail = incidentDetail.reset_index(drop=True)
if sample: #sample params for log file
    fileName = 'BPIC14IncidentDetail_sample.csv'
else:
    fileName = 'BPIC14IncidentDetail.csv'
  
incidentDetail.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")



interaction['start'] = interaction['start'].astype('datetime64[ns]')
interaction['start'] = interaction['start'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S')+'.000+0100')
interaction['timestamp'] = interaction['timestamp'].astype('datetime64[ns]')
interaction['timestamp'] = interaction['timestamp'].map(lambda x: x.strftime('%Y-%m-%dT%H:%M:%S')+'.000+0100')
interaction = interaction.reset_index(drop=True)
interaction = interaction.replace(['#MULTIVALUE','#N/B'], 'Unknown')
if sample: #sample params for log file
    fileName = 'BPIC14Interaction_sample.csv'
else:
    fileName = 'BPIC14Interaction.csv'

interaction.to_csv(path_to_neo4j_import_directory+fileName, index=True, index_label="idx",na_rep="Unknown")



