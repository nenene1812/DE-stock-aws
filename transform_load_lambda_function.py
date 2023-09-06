import json
import boto3
from datetime import datetime 
from io import StringIO 
import pandas as pd 
import time


def lambda_handler(event, context):
    # TODO implement
    s3 = boto3.client('s3')
    Bucket = 'nasdaq-index-etl'
    Key = "raw_data/to_processed/"
    
    nasdaq_data =[]
    vinfast_data = []
    tesla_data = []
    coin_keys = []
    for file in s3.list_objects(Bucket = Bucket, Prefix = Key)['Contents']:
        # print(file['Key'])
        file_key = file['Key']
        if file_key.split('.')[-1] == "json":
            if 'nasdaq_index' in file_key:
                response = s3.get_object(Bucket = Bucket,Key=file_key) 
                content = response['Body']
                nasdaq_data.append(json.loads(content.read()))
                coin_keys.append(file_key)
            elif 'vinfast' in file_key:
                response = s3.get_object(Bucket = Bucket,Key=file_key) 
                content = response['Body']
                vinfast_data.append(json.loads(content.read()))
                coin_keys.append(file_key)
            else:
                response = s3.get_object(Bucket = Bucket,Key=file_key) 
                content = response['Body']
                tesla_data.append(json.loads(content.read()))
                coin_keys.append(file_key)
    list_data  = []
    for data in nasdaq_data: 
        timestamp = data['chart']['result'][0]['timestamp']
        volume = data['chart']['result'][0]['indicators']['quote'][0]['volume']
        close_price = data['chart']['result'][0]['indicators']['adjclose'][0]['adjclose']
        c = 0  
        for i in timestamp: 
            list_data.append({
                'symbol':'NASDAQ:Composite',
                'data_datetime':datetime.fromtimestamp( timestamp[c]).strftime('%Y-%m-%d %H:%M:%S') ,
                'date_timetime_epoch':  timestamp[c],
                'data_date': datetime.fromtimestamp( timestamp[c]).strftime('%Y-%m-%d'),
                'price':close_price[c],
                'change_percent':0 if c==0 else (close_price[c]-close_price[c-1])/close_price[c-1]*100,
                'volume':volume[c]
            })
            c+=1
    
    
    for data in vinfast_data: 
        for key, value in data['data']['time_series'].items():
            list_data.append({
                'symbol':'VFS:NASDAQ',
                'data_datetime':datetime.strptime(key, '%Y-%m-%d %H:%M:%S')  ,
                'date_timetime_epoch':  time.mktime(datetime.strptime(key, '%Y-%m-%d %H:%M:%S').timetuple()),
                'data_date': datetime.strptime(key, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'),
                'price':value['price'],
                'change_percent':value['change_percent'],
                'volume':value.get('volume', 0)
            })
    
    for data in tesla_data: 
        for key, value in data['data']['time_series'].items():
            list_data.append({
                'symbol':'TSLA:NASDAQ',
                'data_datetime': datetime.strptime(key, '%Y-%m-%d %H:%M:%S'),
                'date_timetime_epoch':  time.mktime(datetime.strptime(key, '%Y-%m-%d %H:%M:%S').timetuple()),
                'data_date': datetime.strptime(key, '%Y-%m-%d %H:%M:%S').strftime('%Y-%m-%d'),
                'price':value['price'],
                'change_percent':value['change_percent'],
                'volume':value.get('volume', 0)
            })
    
    nasdaq_data = pd.DataFrame.from_dict(list_data)
    nasdaq_key = 'transformed_data/nasdaq_transformed_' +str(datetime.now())+".csv"
    nasdaq_buffer = StringIO()
    nasdaq_data.to_csv(nasdaq_buffer,index=False)
    nasdaq_content = nasdaq_buffer.getvalue()
    s3.put_object(Bucket=Bucket,Key = nasdaq_key, Body = nasdaq_content)
    
    s3_resource = boto3.resource('s3')
    for key in coin_keys:
        cp_source = {
            'Bucket':Bucket,
            'Key':key
        }
        s3_resource.meta.client.copy(cp_source, Bucket, 'raw_data/processed/'+key.split("/")[-1])
        s3_resource.Object(Bucket,key).delete()