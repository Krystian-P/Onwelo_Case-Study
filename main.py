import os

from keys import access_key, secret_access_key, nasdaq_api_key
import nasdaqdatalink
import boto3
import csv
import pandas as pd

client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

# require "economist_country_codes.csv" - list of avaible contiers on nasdaq server for BIGMAC index 
# date format yyyy-mm-dd
def downloadData(start_date, end_date):
    nasdaqdatalink.ApiConfig.api_key = nasdaq_api_key
    with open('economist_country_codes.csv') as countries:
        spamreader = csv.reader(countries, delimiter='|')
        for row in spamreader:
            data = nasdaqdatalink.get(f'ECONOMIST/BIGMAC_{row[1]}', start_date=start_date, end_date=end_date)
            data.to_excel(f"data\BIGMAC_{row[0]}.xlsx")
            uploadToS3()


def uploadToS3():
    for file in os.listdir('data'):
        if '.xlsx' in file:
            upload_file_bucket = 'casestudyonwelo'
            upload_file_key = '/data' + str(file)
            client.upload_file('data/'+file, upload_file_bucket, upload_file_key)



uploadToS3()