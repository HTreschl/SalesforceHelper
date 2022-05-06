# SalesforceHelper
Tools for manipulating Salesforce instance through python. Includes support SOQL queries, SOSL queries, bulk API update/delete/insert, converting Salesforce reports to dataframes, and general helper functions

# Examples
# Connect to the Salesforce instance
Update the SfLoginInfo with the appropriate credentials.

'''
import salesforceHelper as sfh

api = sfh.apiConnect()
'''

# Perform a simple SOQL query
SOQL query the Salesforce database and return a dataframe of the results

'''
accountQuery = 'select Id, Name from Account'
accountIds = api.apiQuery(accountQuery)
accountIds.head()
'''
