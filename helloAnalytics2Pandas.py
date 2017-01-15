"""Google ANALYTICS API V4 PARSER W/ pandas df and csv export
Sites = yoursitename
metrics = "ga:pageviews",
dimensions = "ga:dayOfWeek,ga:hour,ga:deviceCategory"
"""

import argparse
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
from oauth2client import client
from oauth2client import file
from oauth2client import tools
from pandas import DataFrame, read_csv
import pandas as pd

# Fill P12 file path, service account email and view ID
SCOPES = ['https://www.googleapis.com/auth/analytics.readonly']
DISCOVERY_URI = ('https://analyticsreporting.googleapis.com/$discovery/rest')
KEY_FILE_LOCATION = './myp12file.p12'
SERVICE_ACCOUNT_EMAIL = '123456789@developer.gserviceaccount.com'
VIEW_ID = '123456789'

def initialize_analyticsreporting():
  """Initializes an analyticsreporting service object.

  Returns:
    analytics an authorized analyticsreporting service object.
  """

  credentials = ServiceAccountCredentials.from_p12_keyfile(
    SERVICE_ACCOUNT_EMAIL, KEY_FILE_LOCATION, scopes=SCOPES)

  http = credentials.authorize(httplib2.Http())

  # Build the service object.
  analytics = build('analytics', 'v4', http=http, discoveryServiceUrl=DISCOVERY_URI)
  return analytics


def get_report(analytics):
  # Use the Analytics Service Object to query the Analytics Reporting API V4.
  return analytics.reports().batchGet(
      body={
        'reportRequests': [
        {
          'viewId': VIEW_ID,
          'dateRanges': [{'startDate': '7daysAgo', 'endDate': 'today'}],
          'metrics': [{'expression': 'ga:sessions'},{'expression':'ga:pageviews'}],
          'dimensions':[{'name':'ga:date'},{'name':'ga:dayOfWeek'},{'name':'ga:hour'},{'name':'ga:deviceCategory'}]
        }]
      }
  ).execute()

def print_response(response):
  """Parses and prints the Analytics Reporting API V4 response"""
  dn = []
  for report in response.get('reports', []):
    columnHeader = report.get('columnHeader', {})
    dimensionHeaders = columnHeader.get('dimensions', [])
    metricHeaders = columnHeader.get('metricHeader', {}).get('metricHeaderEntries', [])
    rows = report.get('data', {}).get('rows', [])

    # Loop for each data row
    for row in rows:
      dimensions = row.get('dimensions', [])
      metrics = row.get('metrics', [])

      # Create dict to store dimensionHeaders & dimensions values
      result=dict(zip(dimensionHeaders,dimensions))

      # Loop to add metricsHeader and metrics value
      for i, values in enumerate(metrics):
        for metricHeader, value in zip(metricHeaders, values.get('values')):
          result[metricHeader.get('name')] = value
      #add result to dict
      dn.append(result.copy())

  #convert dn dict in pd dataframes
  df=pd.DataFrame(dn)
  #generate today date for header string and
  todayDate=pd.datetime.today()
  todayDateStr=todayDate.strftime("%Y%m%d%H%M")

  #Add it to pd dataframe and csv header
  df['timestampApiCall']= todayDate
  df['viewID']=VIEW_ID
  df['viewName']='myViewName'
  df.to_csv(todayDateStr+'gaWeeklyMyView.csv', date_format='%Y%m%d')
  print(df)

def main():

  analytics = initialize_analyticsreporting()
  response = get_report(analytics)
  #Print raw API response
  print(response)
  #Print and export to csv parsed data
  print_response(response)

if __name__ == '__main__':
  main()
