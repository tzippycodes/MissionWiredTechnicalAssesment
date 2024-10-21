# MissionWired_Technical_Assessment ReadMe

 This is a file to Extract, Transform, and Load CRM data from an S3 bucket and write it to .csv files locally

The script is run with Python 3.11.2 and Pandas 1.5.3 on Debian GNU/Linux 12 , which are included in the requirements.txt file. To install the requirements locally, use your command line to run: pip install -r /path/to/requirements.txt

This script should be compatible with future versions of Python and Pandas but may run into issues if the links to the CRM data change. Should the location of the data change, this script should be easily adaptable by changing the URLs used to pull the data from S3 bucket on lines 14-16. Should the schema requirements of the final .csv files change, the transform function on line 49 is adaptable to rewrite as needed.

Working with large datasets, changing types and schema, and figuring out solutions to things is fun! If you need to adapt this script moving forward, I hope you enjoy the challenge of discovering the shape it needs to take next!
