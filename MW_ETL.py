# A Python script to convert extract, transform, and load CRM data into "people" and "acquisition_facts" formatted using the assigned schema. Written for the MissionWired technical assessment for the Associate Data Engineer Role.

# Importing the Required Dependencies
import requests
import pandas as pd
from datetime import datetime
import logging as log_file
from pathlib import Path
from collections import OrderedDict

# Defining variables to be used during function calls

# CRM data from S3 buckets
Constituent_Information = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv"
Constituent_Email_Addresses = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv"
Constituent_Subscription_Status = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv"

# Local destination file paths
people_file_path = "people.csv"
acquisition_facts_file_path = "acquisition_facts.csv"

def log_progress(message):
    # Log the given message to the console and to code_log.txt, with a timestamp.
    timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + ', ' + message + '\n')
        print(timestamp + ', ' + message + '\n')

def extract(url, file_name):
    # Retrieve the CSVs from the provided S3 Bucket to Pandas Dataframes
    # Sending an HTTP GET request to the S3 Bucket to extract data and save it as a csv
    response = requests.get(url)

    # Name the export file
    export_file_name = file_name + ".csv"

    # Write the extracted file to a local csv
    export_file_csv = open(export_file_name, "w")
    export_file_csv.write(response.text)
    export_file_csv.close()

    # Read the extracted csv to a Pandas Dataframe for transformation
    df = pd.read_csv(export_file_name)

    return df

def transform(df1, df2, df3):
    # Create the new DataFrames that will be used combine the information from each of the initial DataFrames that hold the extracted data
    people_df = pd.DataFrame()
    acquisition_facts_df = pd.DataFrame()

    # Change the necessary columns from each of the DataFrames to the people DataFrame.
    people_df['email'] = df2['email']
    people_df['code'] = df1['source']
    people_df['is_unsub'] = df3['isunsub']
    people_df['created_dt'] = df1['create_dt']
    people_df['updated_dt'] = df1['modified_dt']

    # Cast datetime to date to obtain desired data type for acquisition_facts
    df1['create_dt'] = pd.to_datetime(df1['create_dt'])
    df1['create_dt'] = df1['create_dt'].map(lambda dt: dt.strftime("%m/%d/%Y"), na_action='ignore')

    # Create ordered dictionary to count occurences of each date for 'aquisitions' column
    counts = OrderedDict()
    for date in df1['create_dt']:
        counts[date] = counts.get(date, 0) + 1
    acquisition_facts_df['acquisition_date'] = list(counts.keys())
    acquisition_facts_df['acquisitions'] = list(counts.values())

    return people_df, acquisition_facts_df

def write_to_csv(df, file_path): 
    # Convert the transformed dataframe into a csv for distribution
    df.to_csv(file_path, header=True) 

# Calling the defined functions to execute the ETL Process

log_progress('Beginning ETL Process')

Constituent_Information_df = extract(Constituent_Information, "Constituent_Information")

log_progress('Constituent Information converted to Dataframe')

Constituent_Email_Addresses_df= extract(Constituent_Email_Addresses, "Constituent_Email_Addresses")

log_progress('Constituent Email Addresses converted to Dataframe')

Constituent_Subscription_Status_df = extract(Constituent_Subscription_Status, "Constituent_Subscription_Status")

log_progress('Constituent Subscription Status converted to Dataframe')

log_progress('Beginning Data Transformation')

people_df, acquisition_facts_df = transform(Constituent_Information_df, Constituent_Email_Addresses_df, Constituent_Subscription_Status_df)

log_progress('Data Transformation Complete')

log_progress('Loading data to destination files')

write_to_csv(people_df, people_file_path)

log_progress('File "people.csv" created.')

write_to_csv(acquisition_facts_df, acquisition_facts_file_path)

log_progress('File "acquisition_facts.csv" created.')

log_progress('Data loaded. Extract, Transform, and Load process complete.')