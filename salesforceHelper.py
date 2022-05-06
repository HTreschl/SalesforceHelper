# -*- coding: utf-8 -*-
"""
Created on Tue Aug 24 10:49:20 2021

@author: htreschl
"""

import os
import pandas as pd
import requests
import json
import time
from io import StringIO
from simple_salesforce import Salesforce
import pandas_dedupe
import re

class apiConnect():
    
    def __init__(self, pswdFile = '/SfLoginInfo.txt' ):
        #get login info
        file = open(os.path.dirname(__file__) + pswdFile,'r')
        lst = file.readlines()
        user = lst[0].replace('\n','')
        pword = lst[1].replace('\n','')
        securityToken = lst[2].replace('\n','')
        clientSecret = lst[3].replace('\n','')
        consumerKey = lst[4].replace('\n','')
        #grab other params from SF
        params = {
            "grant_type": "password",
            "client_id": consumerKey, # Consumer Key
            "client_secret": clientSecret, # Consumer Secret
            "username": user,
            "password": pword + securityToken
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
        '''
        performs an api call to a specified endpoint and returns the response module
        
        INPUTS:
            endpoint: String; api endpoint
        
        Returns: Dataframe
        '''

        url = self.instanceURL+ endpoint
        res = requests.get(url, headers=self.defaultHeaders)
        return res
    

    def bulkApiOperation(self, csv, operation, sobject):
        '''
        performs a bulk operation given an operation type and csv (not in utf-8 format). Column names must match Sobject names
        or API names (for custom objects). requires admin credentials
        
        INPUTS:
            csv: string; file location of csv to be uploaded
            operation: string; operation to be performed
            sobject: string; Salesforce object
        
        RETURNS: 
            String (job ID')
        '''
        
        session = requests.Session()
        
        #create job
        create_endpoint = '/services/data/v52.0/jobs/ingest'
        create_url = self.instanceURL + create_endpoint
        create_body =  json.dumps({'operation' : operation,
                 'object' : sobject, 
                 })

        create = session.post(create_url, data=create_body, headers = self.defaultHeaders)
        #catch errors
        if create.status_code == 400:
            pass
            #return json.loads(create.text)[0]['message']
            
        #upload csv
        upload_headers = {'Content-Type' : 'text/csv',
                          'Accept' :'text/csv',
                          'Authorization': 'Bearer %s' % self.accessToken}
        upload_url = self.instanceURL + '/' + create.json()['contentUrl']
        with open(csv, 'r', encoding='utf-8') as data:
            d = data.read().encode('utf-8')
            put = session.put(upload_url, headers= upload_headers, data = d)

        #close the job and add it to the queued jobs
        if put.status_code == 201:
            print('job created')
            patch_headers = {'Content-Type' : 'application/json; charset=UTF-8',
                             'Accept' : 'application/json',
                             'Authorization': 'Bearer %s' % self.accessToken}
            put_url = self.instanceURL + '/services/data/v52.0/jobs/ingest/{}/'.format(create.json()['id'])    
            put_body = json.dumps({ 'state' : 'UploadComplete'})
            requests.patch(put_url, data=put_body, headers=patch_headers)
            session.close()
            return create.json()['id']
        else:
            session.close()
            print('upload failed:' + json.loads(put.text)[0]['message'])
        
    
    def checkJobSuccessesAndFailures(self, jobID):
        '''
        Inputs:
            jobID: string of the job ID
            
        Returns: textIO,textIO of the successes and failures
        '''
        
        headers = {'Content-Type' : 'application/json; charset=UTF-8',
                             'Accept' : 'application/json',
                             'Authorization': 'Bearer %s' % self.accessToken}
        success_url = self.instanceURL + '/services/data/v52.0/jobs/ingest/{}/successfulResults/'.format(jobID)
        failure_url = self.instanceURL + '/services/data/v52.0/jobs/ingest/{}/failedResults/'.format(jobID)
        successes = requests.get(success_url, headers=headers).text
        failures = requests.get(failure_url, headers=headers).text
        return successes, failures
    
    def getReportJson(self, report_id):
        '''#returns json data from a salesforce report'''
        
        endpoint = '/services/data/v42.0/analytics/reports/' + report_id + '?includeDetails=true'
        rest_api_url = self.instanceURL + endpoint
        response = requests.get(rest_api_url, headers=self.defaultHeaders)
        return response.json()

    def getAllObjects(self):
        '''Returns json file of metadata for all objects in the salesforce org'''
        
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
        '''get metadata about a sobject'''
        
        endpoint = '/services/data/v50.0/sobjects/{}/describe'.format(sobject)
        api_url = self.instanceURL + endpoint
        response = requests.get(api_url, headers=self.defaultHeaders)
        return response.json()

    def apiQuery(self, query):
        '''returns dataframe from a SOQL query
        EX: pass "select Name from Account to return the names of all SF Accounts '''

        create_body =  json.dumps({'operation' : 'query',
                'query' : query, 
                })
        job_url = self.instanceURL + '/services/data/v52.0/jobs/query'
        create = requests.post(job_url, data=create_body, headers=self.defaultHeaders)
        if create.status_code == 400:
            return create.text
        
        #wait for job to complete
        wait_url = job_url + '/{}'.format(create.json()['id'])
        while requests.get(wait_url, headers=self.defaultHeaders).json()['state'] != 'JobComplete':
            time.sleep(5)
        #get csv of results
        results_url = job_url + '/{}/results'.format(create.json()['id'])
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
class joiner():
    
    def __init__(self, df): 
        self.df = df
        self.api = apiConnect()
                   
    def join_users(self, col):
        users = self.api.apiQuery('select Id, Name from User').rename(columns = {'Name' :'User Name', 'Id':'User Id'})
        joined = self.df.merge(users, how='left', left_on = col, right_on = 'User Name')
        joined = joined.drop(columns=['User Name'])
        return joined
    
    def join_firms(self, col):
        accts = self.api.apiQuery('select Id, Name from Account').rename(columns = {'Id':'Account Id'})
        accts['cleanname'] = strip_firm_extras(accts['Name']) #figure out the issue here
        accts = accts.drop(columns = ['Name']).drop_duplicates(subset=['cleanname'])
        self.df['cleanname'] = strip_firm_extras(self.df[col])
        joined = self.df.merge(accts, how='left', on='cleanname')
        joined = joined.drop(columns = ['cleanname'])
        return joined
    
    def join_contacts(self, col):
        conts = self.api.apiQuery('select Id, Email from Contact').rename(columns={'Id':'Contact Id'}).drop_duplicates(subset=['Email'])
        joined = self.df.merge(conts, how='left', left_on = col, right_on = 'Email')
        joined = joined.drop(columns=['Email'])
        return joined


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

 
def get_associated_records(Ids, sobject, match_field, return_fields=['Id']):
    '''returns a dataframe of associated records of type Sobject for a list of Ids'''
    fields = ', '.join(return_fields)
    related = apiConnect().apiQuery('select {},{} from {}'.format(fields, match_field, sobject))
    df = related.merge(Ids, how='inner', left_on=related[match_field], right_on=Ids)
    return df 


def strip_firm_extras(column, pattern='[^\w]|Ltd.*$|Co.*Ltd|LTD|LLC$|L\.L\.C\.|(COM)?[ ]*INC[\.]*$|PLC|AG$|LIMITED|L?L\.?P\.?|FINANCIAL|SERVICES|PTY|SECURIT.*ES|USA|CORP|CORPORATION|Systems|(?<!^)Group|Asia|Markets|International|holdings?|research|^The|Technology|Technol.*gies|Tech.?$|Port.*gues|\(.*\)|OMX$|(?<!^)Trading|Information|Partners|Equity|Americas|(?<!^)Bank|Investments|Tradebook|Master|fund|SFTI|Co. LLC|BD|Finance|(?<!^)NA|Execution|liquidity|GP$|Enterprises?|Fund|Capital'):
        ''' returns a list of firm names stripped of any extra characters'''
        out = [re.sub(pattern, '', name) for name in column]
        out = [elem.upper() for elem in out]
        return out

