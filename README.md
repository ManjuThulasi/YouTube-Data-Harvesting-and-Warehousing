# YouTube-Data-Harvesting-and-Warehousing
**YOUTUBE ELT PROJECT**

**Introduction**
                                   
           YouTube, the online video-sharing platform, has revolutionized the way we consume and interact with media. Launched in 2005, it has grown into a global phenomenon, serving as a hub for entertainment, education, and community engagement. With its vast user base and diverse content library, YouTube has become a powerful tool for individuals, creators, and businesses to share their stories, express themselves, and connect with audiences worldwide.
                                   
**Project Focus On**
                                    
           This project focused on harvesting data from YOUTUBE Channels using YOUTUBE API to process the data and warehouse it. It extracts the particular channel data by using the YouTube channel ID, processes the data, and stores it in the MongoDB database. It has the option to migrate the data to MySQL from MongoDB then analyse the data and give the results depending on the customer questions.
                                     
**Developer Guide**

_**1. Tools Install**_

•	Visual studio code.
•	Jupyter notebook.
•	Python 3.12.0.
•	MySQL.
•	MongoDB.
•	Youtube API key.

_**2. Requirement Libraries to Install**_

•	pip install google-api-python-client, pymongo, mysql-connector-python, sqlalchemy, pymysql, pandas, regex,  streamlit.
(pip install google-api-python-client, pymongo, mysql-connector-python sqlalchemy pymysql, pandas  streamlit )

_**3. Import Libraries**_

**Youtube API libraries**
•	import googleapiclient.discovery
•	from googleapiclient.discovery import build
•	from  googleapiclient.errors import HttpError

**File handling libraries**
•	import re

**Datetime libraries**
•	from datetime import datetime
•	import time

**MongoDB**
•	import pymongo
•	from mongo db import mongo client
•	from pymongo ServerApi import ServerApi
•	import urllib

**SQL libraries**
•	import sqlalchemy
•	from sqlalchemy import create_engine
•	import pymysql

**Pandas** 
•	import pandas as pd

**Dashboard libraries**
•	import streamlit as stl

_**4. E T L Process**_

**a) Extract data**
•	Extract the particular YouTube channel data by using the YouTube channel id, with the help of the YouTube API developer console.

**b) Process and Transform the data**
•	After the extraction process, takes the required details from the extraction data and transform it into JSON format.

**c) Load data**
•	After the transformation process, the data is stored in the MongoDB database, also It has the option to migrate the data to MySQL database from the MongoDB database.

_**5. E D A Process and Framework**_

**a) Access MySQL DB**
•	Create a connection to the MySQL server and access the specified MySQL DataBase by using pymysql library and access tables.

**b) Filter the data**
•	Filter and process the collected data from the tables depending on the given requirements by using SQL queries and transform the processed data into a DataFrame format.

**c) Visualization**
•	Finally, create a Dashboard by using Streamlit and give dropdown options on the Dashboard to the user and select a question from that menu to analyse the data and show the output in Dataframe Table.

6._**User Guide**_

**Step 1. Data collection zone**
•	Search channel_id, copy and paste on the input box and click the Get data and stored button in the Data collection zone.

**Step 2. Data Migrate zone**
•	Click the Migrate to MySQL button to migrate the specific channel data to the MySQL database from MongoDB in the Data Migrate zone.

**Step 3. View the User input Channel ID’s Data**
•	Click the Fetch Channel Details, Fetch Video Details, Fetch Comment Details buttons to get the migrated specific channel id data entered in the Data Collection zone migrated  from  MySQL database from MongoDB in the Data Migrate zone.

**Step 4. Select the Tables for view** 
•	Click the Radio buttons Channels, Videos, Comments to view channel details, video details and comment details of the imported 10 channels data migrated from the  Data Collection zone to Data Migrate zone.

**Step 5. Channel Data Analysis zone**
•	Select a Question from the dropdown option you can get the results in Dataframe format.


