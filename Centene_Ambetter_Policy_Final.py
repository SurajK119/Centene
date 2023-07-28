import json
import requests
import zipfile
import os
from bs4 import BeautifulSoup
from datetime import datetime
from datetime import date
import re
from constants import centene_ambetter
import boto3
import io
import pandas as pd
import csv


di = {
    "www.ambetterofalabama.com" : "Alabama",
     "ambetter.azcompletehealth.com" : "Arizona",
     "ambetter.arhealthwellness.com" : "Arkansas",
     "www.healthnet.com": "NA",
      "ambetter.sunshinehealth.com" : "NA",
      "ambetter.pshpgeorgia.com" : "Georgia",
      "www.ambetterofillinois.com" : "Illinois",
      "ambetter.mhsindiana.com" : "Indiana",
      "ambetter.sunflowerhealthplan.com" : "NA",
      "ambetter.wellcareky.com" : "NA",
      "ambetter.louisianahealthconnect.com" : "Louisiana",
      "www.ambettermeridian.com" : "NA",
      "ambetter.magnoliahealthplan.com" : "NA",
      "ambetter.homestatehealth.com" : "NA",
      "ambetter.nebraskatotalcare.com" : "Nebraska",
      "ambetter.silversummithealthplan.com" : "NA",
      "ambetter.nhhealthyfamilies.com" : "New Hampshire",
      "ambetter.wellcarenewjersey.com": "Newjersey",
      "ambetter.westernskycommunitycare.com" : "NA",
      "www.fideliscare.org" : "NA",
      "www.ambetterofnorthcarolina.com" : "North Carolina",
      "ambetter.buckeyehealthplan.com" : "NA",
      "www.ambetterofoklahoma.com" : "Oklahoma",
      "www.healthnetoregon.com" : "Oregon",
      "ambetter.pahealthwellness.com" : "Pennsylvania",
      "ambetter.absolutetotalcare.com" : "NA",
      "www.ambetteroftennessee.com" : "Tennessee",
      "ambetter.superiorhealthplan.com" : "NA",
      "ambetter.coordinatedcarehealth.com" : "NA"
}

s3_client = boto3.client('s3')

def extract_pdf_links(centene_ambetter):
    
    for url in centene_ambetter:

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
                    if not res.endswith("redirect.html"):
                        if res.endswith(".pdf"):
                            visited_urls.add(res)
        
        for link in list(visited_urls):
            for key in di:
                if key == url.split("/")[2]:
                    
                    # Create a list of dictionaries to hold the data
                    data = {'state' : [di[key]],
                            'site_name': [key],
                            'line_of_business': ['Health Insurance Marketplace'],
                            'type_of_service': ['NA'],
                            'pdf_link': [link],  
                            'download_date': [date.today()],
                            'file_name': [link.split('/')[-1]],
                            'client_name' : ["Centene"]
                            }

            # Define the CSV file path and column names
            csv_file_path = "Ambetter.csv"
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


def save_policy_files_to_s3(centene_ambetter):
    client_name = 'Centene'
    folder_name = 'Medicaid_Policies'
    bucket_name = 'zigna-nsa-payer-data'
    date = date.today()

    extract_pdf_links(centene_ambetter)

    df = pd.read_csv("Ambetter.csv")
    
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