import json
import os
from botocore.vendored import requests
import requests
# import pandas as pd
import time
import boto3
from datetime import datetime 

def lambda_handler(event, context):
    # TODO implement
    # get data NASDAQ:Composite from Yahoo finance
    
    url = "https://apidojo-yahoo-finance-v1.p.rapidapi.com/stock/v3/get-chart"
    
    querystring = {"interval":"1d","symbol":"^IXIC","range":"1mo","region":"US"
                   ,"includePrePost":"false","useYfid":"true","includeAdjustedClose":"true"
                   ,"events":"capitalGain,div,split"}
    
    headers = {
    	"X-RapidAPI-Key":  os.environ.get('XRapidAPIKey'),
    	"X-RapidAPI-Host": "apidojo-yahoo-finance-v1.p.rapidapi.com"
    }
    
    response = requests.get(url, headers=headers, params=querystring)
    
    nasdaq_index = response.json()
    
    # get data Vinfast and Tesla from Yahoo finance
    
    url = "https://real-time-finance-data.p.rapidapi.com/stock-time-series"

    querystring = {"symbol":"VFS:NASDAQ","period":"1M","language":"en"}
    
    headers = {
    	"X-RapidAPI-Key": os.environ.get('XRapidAPIKey'),
    	"X-RapidAPI-Host": "real-time-finance-data.p.rapidapi.com"
    }
    
    response = requests.get(url, headers=headers, params=querystring)
    
    data_vinfast = response.json()
    
    querystring = {"symbol":"TSLA:NASDAQ","period":"1M","language":"en"}

    headers = {
    	"X-RapidAPI-Key": os.environ.get('XRapidAPIKey'),
    	"X-RapidAPI-Host": "real-time-finance-data.p.rapidapi.com"
    }
    
    response = requests.get(url, headers=headers, params=querystring)
    
    data_tesla = response.json()
    
    # send dat to AWS S3
    s3 = boto3.client('s3')
    data = json.dumps(nasdaq_index)
    filename_nasdaq = "rapidapicoin_raw_nasdaq_index"+str(datetime.now()) +'.json'
    s3.put_object(  Body= data, 
                    Bucket="nasdaq-index-etl", 
                    Key="raw_data/to_processed/"+filename_nasdaq)
    
    data = json.dumps(data_vinfast)
    filename_vinfast = "rapidapicoin_raw_vinfast"+str(datetime.now()) +'.json'
    s3.put_object(  Body= data, 
                    Bucket="nasdaq-index-etl", 
                    Key="raw_data/to_processed/"+filename_vinfast)
    
    data = json.dumps(data_tesla)
    filename_tesla = "rapidapicoin_raw_tesla"+str(datetime.now()) +'.json'
    s3.put_object(  Body= data, 
                    Bucket="nasdaq-index-etl", 
                    Key="raw_data/to_processed/"+filename_tesla)
                    