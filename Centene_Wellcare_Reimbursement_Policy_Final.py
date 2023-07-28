import json
import requests
import zipfile
import os
from bs4 import BeautifulSoup
from datetime import datetime
import re
from constants import centene_wellcare_reimbursement
import boto3
import io
import pandas as pd


downloadable_links = []
s3_client = boto3.client('s3')

# Create function to extract html content
def extract_html_content(site):
    
    temp = {'state' : site.split("/")[-4],
            'site_name':"https://www.centene.com/products-and-services/browse-by-state/" + site.split("/")[-4] + ".html",
            'line_of_business':'Medicare (MAPD and PDP)',
            'type_of_service':'NA',
            'pdf_links' : [],
           }
    
    response = requests.get(site)
    response.raise_for_status()  # Raise an exception for non-2xx status codes
    html_content = response.content
    html_text = response.text
    
    soup = BeautifulSoup(html_content, 'html.parser')
    links = soup.find_all('a')
    
    return temp,links

# Create function to extract pdf links
def extract_pdf_links(centene_wellcare_reimbursement):
    l1 = []
    for site in centene_wellcare_reimbursement:
        temp,links = extract_html_content(site)
        
        for link in links:
            res = link.get('href')
            l1.append(res)
            # Get the value of href attribute
            if res is not None and res.startswith("/"):
                res = "https://" + site.split("/")[2] + res
                if "PDFs" in res and not res.endswith(".ashx"):
                    res = res.split("?")[0]

                if "PDFs" in res and res.endswith(".ashx"):
                    res = res.replace(".ashx", ".pdf")
                    temp['pdf_links'].append(res)
        
        if len(temp['pdf_links']) != 0:
            downloadable_links.append(temp)
            
    return downloadable_links    


def save_policy_files_to_s3(centene_wellcare_reimbursement):
    client_name = 'Centene'
    folder_name = 'Wellcare_Reimbursement_Policies'
    bucket_name = 'zigna-nsa-payer-data'
    current_datetime = datetime.now()
    present_date = current_datetime.date()
    date = present_date

    downloadable_links = extract_pdf_links(centene_wellcare_reimbursement)

    # Create final data frame
    df = pd.DataFrame(downloadable_links).explode('pdf_links')
    df = df.drop_duplicates()
    df['download_date'] = date
    df['client_name'] = client_name
    df['file_name'] = df['pdf_links'].apply(lambda x : x.split('/')[-1])

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
            
            
 