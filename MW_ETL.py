# A Python script to convert extract, transform, and load CRM data into "people" and "acquisition_facts" formatted using the assigned schema. Written for the MissionWired technical assessment for the Associate Data Engineer Role.

# Importing the Required Dependencies

import requests
import pandas as pd

# Defining the extract function to retrieve the CSVs from the provided S3 Bucket to Pandas Dataframes

def extract(url):
    # Send an HTTP GET request to the S3 Bucket to extract the csv
    response = request.get(url)

    #Store the extracted csv in a variable
    csv = response.body

    # Reads the extracted csv to a Pandas Dataframe for transformation
    df = pd.read_csv(csv)

    return df

# Defining the transform function 

# Defining the load function

# Defining variables to be used during function calls

Constituent_Information = https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons.csv
Constituent_Email_Addresses = https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email.csv
Constituent_Subscription_Status = https://als-hiring.s3.amazonaws.com/fake_data/2020-07-01_17%3A11%3A00/cons_email_chapter_subscription.csv



# Calling the defined functions to execute the ETL Process