# A Python script to convert extract, transform, and load CRM data into "people" and "acquisition_facts" formatted using the assigned schema. Written for the MissionWired technical assessment for the Associate Data Engineer Role.

#Importing the Required Dependencies
import requests
from bs4 import BeautifulSoup
import pandas as pd
