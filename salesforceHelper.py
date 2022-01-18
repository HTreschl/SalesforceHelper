# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 10:49:20 2021

@author: htreschl
"""

import pandas as pd
import requests
import json
import time
from io import StringIO
import pandas_dedupe

class apiConnect():
    '''a class for connecting to, pulling, and pushing to Salesforce using SOAP and Bulk APIs. Login credentials and security token are required.'''
    
    def __init__(self, user='', pword='', securityToken=''):
        self.username = user
        self.password = pword
        self.securityToken = securityToken
        self.verify = '' #replace with the file location of the SSL certificate, if necessary
        #grab other params from SF
        params = {
            "grant_type": "password",
            "client_id": "", # your consumer Key
            "client_secret": "", # your consumer Secret
            "username": self.username,
            "password": self.password + self.securityToken
        }
        r = requests.post("https://login.salesforce.com/services/oauth2/token", params=params)
        
        self.accessToken = r.json().get("access_token")
        self.instanceURL = r.json().get("instance_url")
        self.defaultHeaders = {
            'Content-type': 'application/json',
            'Accept-Encoding': 'gzip',
            'Authorization': 'Bearer %s' % self.accessToken , 
            'Accept' : 'application/json'
            }
   

    def apiCall(self, endpoint):
        '''performs an api call to a specified endpoint and returns the response module'''

        url = self.instanceURL+ endpoint
        res = requests.get(url, headers=self.defaultHeaders)
        return res
    

    def bulkApiOperation(self, csv, operation, sobject):
        '''performs the specified bulk API operation using the specififed CSV. Column names must match Sobject names
        or API names (for custom objects). returns the job id if the upload is successful and the error if it fails'''
        
        '''requires admin credentials'''
        
        #create job  
        url = self.instanceURL + '/services/data/v52.0/jobs/ingest'
        body =  json.dumps({'operation' : operation,
                 'object' : sobject, 
                 })
        create = requests.post(url, data=body, headers = self.defaultHeaders)
        print(create.text)
        #upload csv to job to create batch
        upload_headers = {'Content-Type' : 'text/csv',
                          'Accept': 'application/json; charset=utf-8',
                          'Authorization': 'Bearer %s' % self.accessToken}
        url = self.instanceURL + '/' + create.json()['contentUrl']
        with open(csv, 'r', encoding = 'utf-8') as data:
            put = requests.put(url, headers= upload_headers, data = data.read().encode(encoding='utf-8'))
        print(put.text)
        #process and the job and perform operation
        if put.status_code == 201:
            patch_headers = {'Content-Type' : 'application/json; charset=UTF-8',
                             'Accept' : 'application/json',
                             'Authorization': 'Bearer %s' % self.accessToken}
            url = self.instanceURL + '/services/data/v45.0/jobs/ingest/{}/'.format(create.json()['id'])    
            body = json.dumps({ 'state' : 'UploadComplete'})
            requests.patch(url, data=body, headers=patch_headers, verify = self.verify)
            return create.json()['id']
        else:
            print('upload failed:' + put.text)
        
    
    def checkJobSuccessesAndFailures(self, jobID):
        '''returns the success/error text of a job ID. Can be written to a CSV for easy viewing if desired'''
        
        headers = {'Content-Type' : 'application/json; charset=UTF-8',
                             'Accept' : 'application/json',
                             'Authorization': 'Bearer %s' % self.accessToken}
        success_url = self.instanceURL + '/services/data/v42.0/jobs/ingest/{}/successfulResults/'.format(jobID)
        failure_url = self.instanceURL + '/services/data/v42.0/jobs/ingest/{}/failedResults/'.format(jobID)
        successes = requests.get(success_url, headers=headers, verify = self.verify)
        failures = requests.get(failure_url, headers=headers, verify = self.verify)
        return successes, failures
    
    def getReportJson(self, report_id):
        '''returns json data from a salesforce report, parsable with get_report_json'''
        
        endpoint = '/services/data/v42.0/analytics/reports/' + report_id + '?includeDetails=true'
        rest_api_url = self.instanceURL + endpoint
        response = requests.get(rest_api_url, headers=self.defaultHeaders)
        return response.json()

    def getAllObjects(self):
        '''Returns dataframe containing metadata for all objects in the salesforce org indexed by object name
        '''
        
        endpoint = '/services/data/v52.0/sobjects/'
        api_url = self.instanceURL + endpoint
        response = requests.get(api_url, headers=self.deaultHeaders, verify = self.verify)
        json = response.json()
        
        #write to dataframe
        rows = []
        for item in json['sobjects']:
            row = []
            for field in item:
                row.append(item[field])
            rows.append(row)
        
    
        df = pd.DataFrame(rows, columns = json['sobjects'][0].keys())
        return df

    def getObjectMetadata(self, sobject):
        '''get all metadata about a sobject. Returns data as json'''
        
        endpoint = '/services/data/v50.0/sobjects/{}/describe'.format(sobject)
        api_url = self.instanceURL + endpoint
        response = requests.get(api_url, headers=self.defaultHeaders)
        return response.json()

    def apiQuery(self, query):
        '''returns dataframe from a SOQL query
        EX: pass "select Name from Account to return the names of all SF Accounts '''

        body =  json.dumps({'operation' : 'query',
                'query' : query, 
                })
        url = self.instanceURL + '/services/data/v52.0/jobs/query'
        create = requests.post(url, data=body, headers=self.defaultHeaders)
        if create.json()['state'] != 'UploadComplete':
            return 'error creating query job'
        
        #wait for job to complete
        while requests.get(url + '/{}'.format(create.json()['id']), headers=self.defaultHeaders).json()['state'] != 'JobComplete':
            time.sleep(5)
        #get csv of results
        results_url = url + '/{}/results'.format(create.json()['id'])
        results = requests.get(results_url, headers=self.defaultHeaders)
        
        #write results
        buf = StringIO(results.text)
        df = pd.read_csv(buf)
        return df
    
    def SOSLQuery(self, search):
        #send a call with operation = search; query
        body =  json.dumps({'operation' : 'search',
                'query' : search, 
                })
        url = self.instanceURL + '/services/data/v52.0/search/?'
        create = requests.get(url, data=body, headers=self.defaultHeaders)
        return create


    def getPopulatedFields(self, sobject, Id):
        ''' Returns dicts of populated and unpouplated fields for a sObject/Id pair 
        This is very slow, avoid iterating if possible'''
        
        endpoint = '/services/data/v42.0/sobjects/{}/{}'.format(sobject, Id)
        api_url = self.instanceURL + endpoint
        response = requests.get(api_url, headers=self.defaultHeaders).json()
        fields = {}
        unpopulated = []
        for key in response:
            if response[key] != None:
                fields[key] = response[key]
            else:
                unpopulated.append(response[key])
        return fields, unpopulated
    
#%% general helper functions
def frame_from_json(json):
    '''Takes JSON from a salesforce report and returns a dataframe.
    Run in conjunction with get_report_json'''
    
    #get the columns
    cols = list(json['reportExtendedMetadata']['detailColumnInfo'].keys())
    cols = [x.lower() for x in cols]
    
    #get the rows
    rows = json['factMap']['T!T']['rows']
    
    #map each row to a column
    df_dict = {}
    for i in range(len(cols)):
        df_dict[cols[i]] = [x['dataCells'][i]['label'] for x in rows] 
    
    df = pd.DataFrame(df_dict)
    return df

def duplicate_finder(sf_object, on=['Name']):
    '''Returns a dataframe of duplicate objects from a specified objects based
    on any criteria. Criteria input as list'''
    
    criteria= ", "
    criteria = criteria.join(on)
    query = 'Select id,{} from {}'.format(criteria, sf_object)
    df = apiConnect.apiQuery(query)
    df['dupes?'] = df.duplicated(subset=(on), keep=False)
    dupes = df[df['dupes?']==True]
    print('There are {} duplicates of the {} object based on {}'.format(len(dupes), sf_object, criteria))
    return dupes

def fuzzy_dupe_finder(sobject, on='Name', conf = .95):
    '''input a sobject and field to fuzzy match on as strings,
    returns datframe of duplicates'''
    
    df = apiConnect().apiQuery(query='select {},Id from {}'.format(on, sobject))
    
    out = pandas_dedupe.dedupe_dataframe(df, ['Name'])
    has_dupes = out[out['confidence'] < conf].sort_values(by=['cluster id'])
    return has_dupes
