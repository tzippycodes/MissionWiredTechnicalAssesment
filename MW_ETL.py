# A Python script to convert extract, transform, and load CRM data into "people" and "acquisition_facts" formatted using the assigned schema. Written for the MissionWired technical assessment for the Associate Data Engineer Role.

# Importing the Required Dependencies

import requests
import pandas as pd
from datetime import datetime
import logging as log_file

# Defining the Logging Function
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S' #Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("code_log.txt", "a") as f:
        f.write(timestamp + ',' + message + '\n')

# Defining the extract function to retrieve the CSVs from the provided S3 Bucket to Pandas Dataframes

def extract(url, file_name):
    # Send an HTTP GET request to the S3 Bucket to extract data and save it as a csv
    response = requests.get (url)

    export_file_name = file_name + ".csv"

    export_file_csv = open(export_file_name, "w")

    export_file_csv.write(response.text)

    export_file_csv.close()

    # Reads the extracted csv to a Pandas Dataframe for transformation
    df = pd.read_csv(export_file_name)

    return df

# Defining the transform function 

# Defining the load function

# Defining variables to be used during function calls

Constituent_Information = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv"
Constituent_Email_Addresses = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv"
Constituent_Subscription_Status = "https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv"


# Calling the defined functions to execute the ETL Process

log_progress('Beginning ETL Process')

df = extract(Constituent_Information, "Constituent_Information")
log_progress('Constituent Information converted to Dataframe')

df2 = extract(Constituent_Email_Addresses, "Constituent_Email_Addresses")
log_progress('Constituent Email Addresses converted to Dataframe')

df3 = extract(Constituent_Subscription_Status, "Constituent_Subscription_Status")
log_progress('Constituent Subscription Status converted to Dataframe')

print(df.head)
print(df2.head)
print (df3.head)

#log_progress('Beginning Data Transformation')

#log_progress('Data Transformation Complete')

#log_progress('Loading data to destination files')

#log_progress('Data loaded. Extract, Transform, and Load process complete.')