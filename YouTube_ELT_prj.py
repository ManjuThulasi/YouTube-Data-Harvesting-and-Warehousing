

#****************************************************** YOUTUBE ELT PROJECT BY MANJU CJ *******************************************************#

#_IMPORTING LIBARIES_#

import streamlit as stl
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from datetime import datetime
import time
import re
import pymongo
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import urllib
import pandas as pd
import pymysql
import sqlalchemy
from sqlalchemy import create_engine

#_STREAMLIT SETUP FOR PAGE TITLE_#

stl.set_page_config(page_title="Youtube ETL Project", page_icon=":star2:", layout="wide", initial_sidebar_state="auto", menu_items=None)

stl.title(":red[YouTube ] :blue[ELT project]  :red[Dashboard] 📡")
stl.subheader(":orange[DATA MIGRATION FROM API TO SQL] ⌛")
stl.header(':violet[Data collection zone]')
stl.write ('(Note:- This zone **collect data** from **YOUTUBE API** by using channel id and **stored it in the :green[MongoDB] database**.)')

#*******************************************IMPORING DATA FROM YOUTUBE API USING PYTHON SCRIPTING ********************************************#

#_API-KEY-CONNECTION_#

def Api_connect():
    Api_Id="AIzaSyB6RBTN5M05VI1nruOJWRfZqSRnR4uERwc"

    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    return youtube

youtube=Api_connect()


#_TO GET CHANNEL ID THROUGH CHANNEL NAME_#

channel_id= input("Enter the channel name:")
request = youtube.search().list(
          part = "id,snippet",
          channelType="any",
          maxResults =1,
          q= channel_id
)
response = request.execute()
response

channel_ids=['UCHPkiBUeR3NPCpF-toBd0PQ',
             'UC9fx2q5eaB7SG1eIB9VNvnQ',
             'UC6TlN-P52_a6fA5o9UrjFsg',
             'UCXo3hl1b96Pdo18uR9gtYGg',
             'UCQpgJad_YaHAW_CVFTBNyiw',
             'UCQbXrhx6gK42Bl9T3me7Zvw',
             'UC6i4-GuVpkcR_1UbgfldM_g',
             'UCuS7xIoXRFWnCSwDsrne2Mg',
             'UCF6H0li8VwQ9BzmVYRQKvqg',
             'UCR0ZIyQgfkpain4spsvKAMg'
             ]


channel_id = stl.text_input("Enter the channel Id")

#_FUNCTION TO GET CHANNEL STATISTICS_#

def channel_statistics(youtube,channel_ids):
    all_data = []
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics",
    id=channel_ids)
    response = request.execute()

    for i in range(len(response["items"])):
        data = dict(channel_id = response["items"][i]["id"],
                    channel_name = response["items"][i]["snippet"]["title"],
                    channel_views = response["items"][i]["statistics"]["viewCount"],
                    subscriber_count = response["items"][i]["statistics"]["subscriberCount"],
                    total_videos = response["items"][i]["statistics"]["videoCount"],
                    Channel_Description = response["items"][i]["snippet"]["description"],
                    playlist_id = response["items"][i]["contentDetails"]["relatedPlaylists"]["uploads"])
        all_data.append(data)
    return all_data

channel_details = channel_statistics(youtube,channel_ids)
channel_df = pd.DataFrame(channel_details)


#_FUNCTION TO GET PLAYLIST ID_#

def get_playlist_data(channel_df):
    playlist_ids = []

    for i in channel_df["playlist_id"]:
        playlist_ids.append(i)

    return playlist_ids

playlist_id_data = get_playlist_data(channel_df)

#_FUNCTION TO GET VIDEO ID_#

def get_video_ids(youtube,playlist_id_data):
    video_id = []

    for i in playlist_id_data:
        next_page_token = None
        more_pages = True

        while more_pages:
            request = youtube.playlistItems().list(
                        part = 'contentDetails',
                        playlistId = i,
                        maxResults = 50,
                        pageToken = next_page_token)
            response = request.execute()

            for j in response["items"]:
                video_id.append(j["contentDetails"]["videoId"])

            next_page_token = response.get("nextPageToken")
            if next_page_token is None:
                more_pages = False
    return video_id

video_id = get_video_ids(youtube,playlist_id_data)

#_FUNCTION TO GET VIDEO STATISTICS_#

def get_video_details(youtube,video_id):

    all_video_stats = []

    for i in range(0,len(video_id),50):

        request = youtube.videos().list(
                  part="snippet,contentDetails,statistics",
                  id = ",".join(video_id[i:i+50]))
        response = request.execute()

        for video in response["items"]:
            published_dates = video["snippet"]["publishedAt"]
            parsed_dates = datetime.strptime(published_dates,'%Y-%m-%dT%H:%M:%SZ')
            format_date = parsed_dates.strftime('%Y-%m-%d')

            videos = dict(video_id = video["id"],
                         channel_id = video["snippet"]["channelId"],
                         video_name = video["snippet"]["title"],
                         published_date = format_date ,
                         view_count = video["statistics"].get("viewCount",0),
                         Description = video['snippet']['description'],
                         like_count = video["statistics"].get("likeCount",0),
                         Thumbnail = video['snippet']['thumbnails']['default']['url'],
                         Favorite_Count = video['statistics']['favoriteCount'],
                         comment_count= video["statistics"].get("commentCount",0),
                         duration = video["contentDetails"]["duration"],
                         Caption_Status = video['contentDetails']['caption'])
            all_video_stats.append(videos)


    return (all_video_stats)

video_details = get_video_details(youtube,video_id)

video_ids = [video['video_id'] for video in video_details]


#_FUNCTION TO GET VIDEO COMMENTS STATISTICS_#


def get_video_comments(youtube, video_ids):
    comments_data = []
    try:
        next_page_token = None
        for i in video_ids:
            while True:
                try:
                    request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=i,
                        textFormat="plainText",
                        maxResults=100,
                        pageToken=next_page_token
                    )
                    response = request.execute()

                    for item in response["items"]:
                        published_date = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                        parsed_dates = datetime.strptime(published_date, '%Y-%m-%dT%H:%M:%SZ')
                        format_date = parsed_dates.strftime('%Y-%m-%d')

                        comment_text = item["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
                        cleaned_comment_text = re.sub(r'[^a-zA-Z0-9\s]', '', comment_text)

                        comments = dict(
                            comment_id=item["id"],
                            video_id=item["snippet"]["videoId"],
                            comment_text=cleaned_comment_text,
                            comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            comment_published_date=format_date
                        )
                        comments_data.append(comments)

                    next_page_token = response.get('nextPageToken')
                    if next_page_token is None:
                        break

                except HttpError as e:
                    if e.resp.status == 403 and "commentsDisabled" in e.content.decode('utf-8'):
                        print(f"Comments are disabled for video with id {i}. Skipping...")
                        break
                    else:
                        raise

    except Exception as e:
        print(f"An error occurred: {e}")

    return comments_data

channel_details = channel_statistics(youtube, channel_ids)
playlist_id_data = get_playlist_data(channel_df)
video_id = get_video_ids(youtube, playlist_id_data)
video_details = get_video_details(youtube, video_id)
comment_details = get_video_comments(youtube, video_ids)


#************************************************************_INSERTING DATA INRO MONGO DB ***************************************************#



#_CONNECTING TO MONGO DB USING ENGINE_#

username = "manju" 
password = "Varahi@1155" 

encoded_username = urllib.parse.quote_plus(username)
encoded_password = urllib.parse.quote_plus(password)

uri = f"mongodb+srv://manjuthulasi20:{encoded_password}@cluster0.vdct6ww.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri)
db = client["MY_YOUTUBE_DB"]
col1 = db['CHANNEL_DATA']
col2 = db['VIDEO_DATA']
col3 = db['COMMENT_DATA']

col1.insert_one({"channel_information": channel_details})
col2.insert_one({"video_information": video_details})
col3.insert_one({"comment_information": comment_details}) 

stl.write('''Get data and stored it in the MongoDB database to click below **:blue['Get data and stored']**.''')
Get_data  = stl.button('**Get data and stored**')
if Get_data :
    with stl.spinner('Please wait '):
         time.sleep(5)
         stl.success('Done!, Data Fetched Successfully')
    with stl.spinner('Please wait '):
         time.sleep(5)
         stl.success('Done!, Data Uploaded to MONGO DB Successfully')

   
stl.header(':violet[Data Migrate zone]')



#********************************************************_RETRIVING DATA FROM MONGO DB AS DATAFRAME_********************************************************#


#_CONNECTING TO MONGO DB USING ENGINE_#
username = "manju"
password = "Varahi@1155"

encoded_username = urllib.parse.quote_plus(username)
encoded_password = urllib.parse.quote_plus(password)

uri = f"mongodb+srv://manjuthulasi20:{encoded_password}@cluster0.vdct6ww.mongodb.net/?retryWrites=true&w=majority"

client = MongoClient(uri)
db = client["MY_YOUTUBE_DB"]
col1 = db['CHANNEL_DATA']
col2 = db['VIDEO_DATA']
col3 = db['COMMENT_DATA']

channel_DA = col1.find({}, {"_id": 0, "channel_information": 1})
channel_data = [entry["channel_information"] for entry in channel_DA]

video_DA = col2.find({}, {"_id": 0, "video_information": 1})
video_data = [entry["video_information"] for entry in video_DA]

comment_DA = col3.find({}, {"_id": 0, "comment_information": 1})
comment_data = [entry["comment_information"] for entry in comment_DA]


#_CREATING AS A DATAFRAME_#

channel_Df = pd.DataFrame(channel_data)
video_Df = pd.DataFrame(video_data)
comment_Df = pd.DataFrame(comment_data)


#***************************************************_INSERTING DATAFRAME TO MY SQL_*********************************************************#

#_CONNECTING TO MY SQL_#

def mysql_connection(channel_Df,video_Df,comment_Df):
    host = "localhost"
    user = "root"
    password = "1155"
    database = "youtube_etl_project"

    connection = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')
    
    channel_Df.to_sql(
       name="channel",
       con=connection,
       if_exists="append",
       index=False
    )

    video_Df.to_sql(
       name="video",
       con=connection,
       if_exists="append",
       index=False
    )

    comment_Df.to_sql(
        name="comment",
        con=connection,
        if_exists="replace",  
        index=False)
    
    return "Success"
#**********************************************_RETRIVING DATA FROM MY SQL TABEL_*************************************************************#

#_CONNECTING TO MY SQL_#

host = "localhost"
user = "root"
password = "1155"
database = "youtube_etl_project"

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')


channel_tabel = 'channel'
video_tabel = 'video'
comment_tabel = 'comment'

#_IMPORTING SQL TABEL_#

channel_df = pd.read_sql(channel_tabel, con=engine)
video_df = pd.read_sql(video_tabel, con=engine)
comment_df = pd.read_sql(comment_tabel, con=engine)

stl.write ('''(Note:- This zone specific channel data **Migrate to :blue[MySQL] database from  :green[MongoDB] database** depending on your selection,
                if unavailable your option first collect data.)''')
stl.write('''Migrate to MySQL database from MongoDB database to click below **:blue['Migrate to MySQL']**.''')
Migrate  = stl.button('**Migrate to MySQL**') 
if Migrate :
    with stl.spinner('Please wait '):
         time.sleep(5)
         stl.success('Done!, Data Migrated Successfully')

stl.subheader(":violet[View the user input channel ID's DATA] ")

#_FUNCTIONS TO FETCH CHANNEL DETAILS FROM MY SQL IN STREAMLIT DASHBORAD_#

def fetch_channel_details(channel_id):
    return channel_df[channel_df['channel_id'] == channel_id]


def fetch_video_details(channel_id):
    return video_df[video_df['channel_id'] == channel_id]


def fetch_comment_details(channel_id):
    video_ids = video_df[video_df['channel_id'] == channel_id]['video_id'].tolist()
    comments1 = comment_df[comment_df['video_id'].isin(video_ids)]
    return comments1

if stl.button("Fetch Channel Details"):
    channel_details = fetch_channel_details(channel_id)
    if not channel_details.empty:
        stl.subheader("Channel Details:")
        stl.write(channel_details)
    else:
        stl.warning("Channel not found. Please enter a valid Channel ID.")


if stl.button("Fetch Video Details"):
    video_details = fetch_video_details(channel_id)
    if not video_details.empty:
        stl.subheader("video_details:")
        stl.write(video_details)
    else:
        stl.warning("video not found. Please enter a valid Channel ID.")


if stl.button("Fetch Comment Details"):
    comment_details = fetch_comment_details(channel_id)
    if not comment_details.empty:
        stl.subheader("comment_details:")
        stl.write(comment_details)
    else:
        stl.warning("comments not found. Please enter a valid Channel ID.")

   
#_FUNCTION TO VIEW SQL CHANNEL DETAIL TABLES IN STREAM LIT_#

stl.subheader(":violet[SELECT THE TABLE FOR VIEW] ")
show_table = stl.radio("CLICK THE BUTTONS",(":green[channels]",":red[videos]",":blue[comments]"))

if show_table == ":green[channels]":
    stl.write(channel_df)
elif show_table ==":red[videos]":
    stl.write(video_df)
elif show_table == ":blue[comments]":
    stl.write(comment_df)

with stl.sidebar:
    stl.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    stl.header("SKILL TAKE AWAY")
    stl.caption("API Integration")
    stl.caption('Python scripting')
    stl.caption("Data Collection")
    stl.caption("MongoDB")
    stl.caption("Data Managment using MongoDB and SQL")

#******************************************************_YOUTUBE DATA ANALYSIS_*************************************************************#


#_EXECUTING THE QUERY TO DISPLAY RESULT_#

if 'query' in locals():
    result = pd.read_sql_query(query, engine)
    stl.write(result)

#_MYSQL CONNECTION_#

host = "localhost"
user = "root"
password = "1155"
database = "youtube_etl_project"

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

st.header(':violet[Channel Data Analysis zone]')
st.write ('''(Note:- This zone **Analysis of a collection of channel data** depends on your question selection and gives a table format output.)''')


#_EXECUTING QUERY FOR THE USER TO SELECT THE QUESTION IN SRTEAMLIT_#

question = st.selectbox(
    'Please Select Your Question',
    ('1. All the videos and the Channel Name',
     '2. Channels with the most number of videos',
     '3. 10 most viewed videos',
     '4. Comments in each video',
     '5. Videos with the highest likes',
     '6. Likes of all videos',
     '7. Views of each channel',
     '8. Videos published in the year 2022',
     '9. Average duration of all videos in each channel',
     '10. Videos with the highest number of comments'))

#_EXECUTING THE ANSWER FOR SELETED QUESTION USING MYSQL QUERY TO EXECUTE THE ANSWER IN STREAMLIT_#

if question == '1. All the videos and the Channel Name':
    query = "SELECT v.video_name AS VideoName, c.channel_name AS ChannelName FROM video v JOIN channel c ON v.channel_id = c.channel_id;"
elif question == '2. Channels with the most number of videos':
    query = "SELECT Channel_Name AS ChannelName, Total_Videos AS NO_Videos FROM channel ORDER BY Total_Videos DESC;"
elif question == '3. 10 most viewed videos':
    query = "SELECT  c.channel_name AS ChannelName,v.video_name AS VideoName ,v.view_count FROM video v JOIN channel c ON v.channel_id = c.channel_id WHERE v.view_count IS NOT NULL ORDER BY v.view_count DESC LIMIT 10;"
elif question == '4. Comments in each video':
    query = "SELECT video_name AS VideoName, comment_count AS Comments FROM video WHERE comment_count >0;"
elif question == '5. Videos with the highest likes':
    query = "SELECT c.channel_name AS ChannelName, v.video_name AS VideoName, v.like_count AS Likes FROM video v JOIN channel c ON v.channel_id =c.channel_id WHERE v.like_count > 0 ORDER BY v.like_count DESC;"
elif question == '6. Likes of all videos':
    query = "SELECT video_name AS VideoName, like_count AS Likes FROM video;"
elif question == '7. Views of each channel':
    query = "SELECT channel_name AS ChannelName, channel_views AS Total_Views FROM channel;"
elif question == '8. Videos published in the year 2022':
    query = "SELECT c.channel_name AS ChannelName, v.video_name AS VideoName, v.published_date AS Published_on_2022 FROM video v JOIN channel c ON v.channel_id =c.channel_id  WHERE EXTRACT(YEAR FROM v.published_date )= 2022;"
elif question == '9. Average duration of all videos in each channel':
    query = "SELECT c. channel_name AS ChannelName, AVG(duration) AS AverageDuration FROM video v JOIN channel c ON v.channel_id = c.channel_id GROUP BY v.channel_id;"
elif question == '10. Videos with the highest number of comments':
    query = "SELECT video_name AS VideoName, comment_count AS TotalComments FROM video WHERE comment_count > 0 ORDER BY comment_count DESC ;"

# Execute the query and display the result as a table
if 'query' in locals():
    result_df = pd.read_sql_query(query, engine)
    stl.table(result_df)

#********************************************END OF MY PROJECT***********************************************#    
