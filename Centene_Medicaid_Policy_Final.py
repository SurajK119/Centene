import json
import requests
import zipfile
import os
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import date
import re
from constants import centene_medicaid
import boto3
import io
import pandas as pd
import csv

di = {
    "www.azcompletehealth.com" : "Arizona",
    "www.care1staz.com" : "Arizona",
    "www.arkansastotalcare.com" : "Arkansas",
    "www.healthnet.com" : "NA",
    "www.cahealthwellness.com" : " California",
    "www.mhn.com" : "NA",
    "www.delawarefirsthealth.com" : "Delaware",
    "www.sunshinehealth.com" : "NA",
    "www.pshpgeorgia.com" : "Georgia",
    "www.ohanahealthplan.com" : "NA",
    "www.ilmeridian.com" : "NA",
    "www.ilyouthcare.com" : "NA",
    "www.mhsindiana.com" : "Indiana",
    "www.iowatotalcare.com" : "Iowa",
    "www.sunflowerhealthplan.com" : "NA",
    "www.wellcareky.com" : "Kentucky",
    "www.louisianahealthconnect.com" : "Louisiana",
    "mmp.mimeridian.com" : "NA",
    "www.magnoliahealthplan.com" : "NA",
    "www.homestatehealth.com" : "NA",
    "www.nebraskatotalcare.com" : "Nebraska",
    "www.silversummithealthplan.com" : "NA",
    "www.nhhealthyfamilies.com" : "New Hampshire",
    "www.wellcarenewjersey.com" : "New Jersey",
    "www.westernskycommunitycare.com" : "NA",
    "www.fideliscare.org" : "NA",
    "network.carolinacompletehealth.com" : "NA",
    "network.carolinacompletehealth.com" : "NA",
    "www.buckeyehealthplan.com" : "NA",
    "www.oklahomacompletehealth.com" : "Oklahoma",
    "www.trilliumohp.com" : "NA",
    "www.pahealthwellness.com" : "NA",
    "www.absolutetotalcare.com" : "NA",
    "www.superiorhealthplan.com" : "NA",
    "www.coordinatedcarehealth.com" : "NA",
    "www.mhswi.com" : "NA"
    }

s3_client = boto3.client('s3')

def extract_pdf_links(centene_medicaid):

    for url in centene_medicaid:

        response = requests.get(url)
        html_content = response.content
        soup = BeautifulSoup(html_content, "html.parser")
        links = soup.find_all("a")
        visited_urls = set()

        for link in links:
            res = link.get("href")
            if res is not None:
                if res.startswith("/"):
                    res = "https://" + url.split("/")[2] + res
                # if not res.endswith("redirect.html"):
                    if res.endswith(".pdf"):
                        visited_urls.add(res)
            
        for link in list(visited_urls):

            if url.split("/")[2] == "www.wellcare.com":
                data = {'state' : [url.split("/")[-4]],
                        'site_name': ["www.wellcare.com"],
                        'line_of_business': ['Medicaid'],
                        'type_of_service': ['NA'],
                        'pdf_link': [link],
                        'download_date': [date.today()],                 
                        'file_name': [link.split('/')[-1]],
                        'client_name' : ["Centene"]                  
                        }
                    
            else:
                for key in di:
                    if key == url.split("/")[2]:
                        # Create a list of dictionaries to hold the data
                        data = {'state' : [di[key]],
                                'site_name': [key],
                                'line_of_business': ['Medicaid'],
                                'type_of_service': ['NA'],
                                'pdf_link': [link],
                                'download_date': [date.today()],
                                'file_name': [link.split('/')[-1]],
                                'client_name' : ["Centene"]
                                }

            # Define the CSV file path and column names
            csv_file_path = "Medicaid.csv"
            column_names = ['state', 'site_name', 'line_of_business', 'type_of_service', 'pdf_link', 'download_date', 'file_name', 'client_name' ]

            # Check if file exists
            file_exists = os.path.isfile(csv_file_path)

            # Create and write to the CSV file
            with open(csv_file_path, 'a', newline='') as csvfile:
                writer = csv.writer(csvfile)

                # Write the header row if the file does not exist
                if not file_exists:
                    writer.writerow(column_names)

                # Write the data rows
                for row_data in zip(*[data[column] for column in column_names]):
                    writer.writerow(row_data)

            # print(f'Data appended to CSV file: {csv_file_path}') 
    return csv_file_path


def save_policy_files_to_s3(centene_medicaid):
    client_name = 'Centene'
    folder_name = 'Medicaid_Policies'
    bucket_name = 'zigna-nsa-payer-data'
    date = date.today()

    extract_pdf_links(centene_medicaid)

    df = pd.read_csv("Medicaid.csv")
    
    # Create a csv file
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    out_filename = f'{client_name}/{folder_name}/policies.csv'
    s3_client.upload_fileobj(csv_buffer, bucket_name, out_filename)    

    batch_size = 1000
    batches = [df[i:i+batch_size] for i in range(0, len(df), batch_size)]
    # Store each batch in S3
    destination_bucket = bucket_name
    for i, batch in enumerate(batches):
        batch_key = f'{folder_name}/{date}/batch/batch_{i}.csv'  # Define a key for each batch in S3
        # Create a csv file
        batch_csv_buffer = io.BytesIO()
        batch.to_csv(batch_csv_buffer, index=False)
        batch_csv_buffer.seek(0)
        # Upload the batch file to S3
        s3_client.upload_file(batch_csv_buffer, destination_bucket, batch_key) 