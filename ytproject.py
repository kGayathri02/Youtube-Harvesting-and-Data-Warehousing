from googleapiclient.discovery import build
import os
from dotenv import load_dotenv
import mysql.connector
import pandas as pd
import streamlit as st
from datetime import datetime
from dateutil import parser


conn = mysql.connector.connect(
    host="127.0.0.1",
    user="root",
    password="Gayujana",
    database="yh"
)
mycursor = conn.cursor()


load_dotenv() 

youtube = build('youtube', 'v3', developerKey='AIzaSyBAI_ESxIyrzju-sCh8C-53z_jObcHWU6k')

def get_channel_details(channel_id):
    request = youtube.channels().list(
        id = channel_id,
        part = 'snippet,contentDetails,statistics,status'
        )
    response = request.execute()

    for i in response['items']:
        data = dict(           
            channel_id = i['id'],
            channel_name = i['snippet']['title'],
            sub_count = i['statistics']['subscriberCount'],
            view_count = i['statistics']['viewCount'],
            videos_count = i['statistics']['videoCount'],
            channel_description = i['snippet']['description'],
            playlist_id = i['contentDetails']['relatedPlaylists']['uploads'],
            channel_status = i['status']['privacyStatus']
            )
    return data

#Getting Video Id's
def get_video_ids(channel_id):
    channel_details = get_channel_details(channel_id)
    unique_playlist_id = channel_details['playlist_id']
    video_ids = []
    nextPageToken = None
    while True:
        response = youtube.playlistItems().list(
            playlistId = unique_playlist_id, 
            part = 'snippet,id',
            maxResults = 50,
            pageToken = nextPageToken
        ).execute()
        for item in response['items']:
            video_ids.append(item['snippet']['resourceId']['videoId'])
        nextPageToken = response.get('nextPageToken')
        if(nextPageToken is None):
            break
    return video_ids

def durationInSeconds(duration):
    duration =list(duration)
    del duration[0:2]
    duration_seconds = 0
    for i,e in enumerate(duration):
        if(e == 'H'):
            duration_seconds += int(duration[i-1])* 60 * 60
        elif(e == 'M'):
            duration_seconds += int(duration[i-1])* 60
        elif(e == 'S'):
            duration_seconds += int(duration[i-1])
    return duration_seconds

#Changing Date format
def changeDateFormat(date_string):
    datetime_obj = parser.isoparse(date_string)
    format_datetime = datetime_obj.strftime('%Y-%m-%d %H:%M:%S')
    return format_datetime

#Getting video details         
def get_video_details(video_ids):
    video_details = []  
 
    for i in range(0, len(video_ids), 50):
        response = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=','.join(video_ids[i:i + 50])).execute()
        
        for item in response['items']:
            data = dict(
                video_id = item.get('id'),
                channel_id = item['snippet'].get('channelId'), 
                video_name = item['snippet'].get('title'),
                video_description = item['snippet'].get('description'),
                published_date = item['snippet'].get('publishedAt'),
                view_count = item['statistics'].get('viewCount'),
                like_count = item['statistics'].get('likeCount'),
                favorite_count = item['statistics'].get('favoriteCount'),
                comment_count = item['statistics'].get('commentCount'),
                duration = item['contentDetails'].get('duration') ,                
                thumbnail = item['snippet']['thumbnails']['default']['url'],                
                caption_status = item['contentDetails'].get('caption')                
            )
            date_string = data['published_date'] 
            date_string = changeDateFormat(date_string)
            data['published_date'] = date_string
            duration = data['duration']
            duration = durationInSeconds(duration)
            data['duration'] = duration                
            video_details.append(data)
    return video_details


def get_comment_details(video_ids):
    try:        
        comment_details = []
        for video_id in video_ids:
            nextPageToken = None
            while True:            
                response = youtube.commentThreads().list(
                    videoId = video_id,
                    part = 'snippet',
                    maxResults = 100,
                    pageToken = nextPageToken                        
                    ).execute()
                for item in response['items']:
                    data = dict(
                        comment_id = item.get('id'),
                        video_id = item['snippet'].get('videoId'),
                        comment_text = item['snippet']['topLevelComment']['snippet'].get('textDisplay'),
                        comment_author = item['snippet']['topLevelComment']['snippet'].get('authorDisplayName'),
                        comment_published_date =  item['snippet']['topLevelComment']['snippet'].get('publishedAt')
                    )
                    comment_details.append(data)
                nextPageToken = response.get('nextPageToken')
                if(nextPageToken is None):
                    break
    except Exception as err:
        print(err)
        pass
    return  comment_details

def get_playlist_details(channel_id):
    next_page_token = None
    all_data = []
    while True:
        request = youtube.playlists().list(
            part='snippet,contentDetails',
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response.get('items', []): 
            data = {
                'Playlist_Id': item['id'],
                'Title': item['snippet']['title'],
                'Channel_Id': item['snippet']['channelId'],
                'Channel_Name': item['snippet']['channelTitle'],
                'PublishedAt': item['snippet']['publishedAt'],
                'Video_Count': item['contentDetails']['itemCount']
            }
            all_data.append(data)
        next_page_token = response.get('nextPageToken')  
        if not next_page_token:
            break
    return all_data

#Creating Tables in MYSQL
def create_tables():
    mycursor.execute("Create Table IF NOT EXISTS Channel (channel_id varchar(255) PRIMARY KEY, channel_name varchar(255) NOT NULL, sub_count int, channel_views int, channel_description text,channel_status varchar(255))")
    mycursor.execute("Create Table IF NOT EXISTS Video (video_id varchar(255) PRIMARY KEY, channel_id varchar(255) NOT NULL, video_name varchar(255), video_description text, published_date datetime, view_count int, like_count int, favorite_count int, comment_count int, duration int, thumbnail varchar(255), caption_status varchar(255), FOREIGN KEY (channel_id) REFERENCES Channel(channel_id))")
    mycursor.execute("Create Table IF NOT EXISTS Comment (comment_id varchar(255) PRIMARY KEY, video_id varchar(255) NOT NULL,comment_text text, comment_author varchar(255),comment_published_date datetime,FOREIGN KEY (video_id) REFERENCES Video(video_id))")
    mycursor.execute("CREATE TABLE IF NOT EXISTS Playlist ( Playlist_Id VARCHAR(100) PRIMARY KEY, Title VARCHAR(100), Channel_Id VARCHAR(100), Channel_Name VARCHAR(100), PublishedAt TIMESTAMP, Video_Count INT)")
    conn.commit()


def show_channel_table():
    mycursor.execute('Select * from Channel')
    myResult = mycursor.fetchall()
    df = pd.DataFrame(data = myResult, columns = mycursor.column_names)
    st.table(df)

def show_video_table():
    mycursor.execute('Select * from Video')
    myResult = mycursor.fetchall()
    df = pd.DataFrame(data = myResult, columns = mycursor.column_names)
    st.table(df)

def show_comment_table():
    mycursor.execute('Select * from Comment')
    myResult = mycursor.fetchall()
    df = pd.DataFrame(data = myResult, columns = mycursor.column_names)
    st.table(df)

def show_playlist_table():
    mycursor.execute('Select * from Playlist')
    myResult = mycursor.fetchall()
    df = pd.DataFrame(data = myResult, columns = mycursor.column_names)
    st.table(df)

# Inserting data to MYSQL
def insert_all_table(channel_id):
    insert_channel_details(channel_id) 
    insert_video_details(channel_id) 
    insert_comment_details(channel_id)
    playlist_data = get_playlist_details(channel_id)
    insert_playlist_details(playlist_data)
          
def insert_channel_details(channel_id):
    channel_details = get_channel_details(channel_id)
    channel_details = (channel_details['channel_id'],channel_details['channel_name'],channel_details['sub_count'],channel_details['view_count'],channel_details['channel_description'],channel_details['channel_status'],)
    insert_query = '''INSERT INTO Channel 
                    VALUES(%s,%s,%s,%s,%s,%s)''' 
    
    check_query = '''SELECT channel_id FROM Channel WHERE channel_id = %s'''
    mycursor.execute(check_query, (channel_details[0],))
    existing_channel = mycursor.fetchone()
    if existing_channel:
        print("Channel already exists in the database.")
    else:
        mycursor.execute(insert_query,channel_details)
        conn.commit()
        print(mycursor.rowcount,'rows inserted successfully')

def insert_video_details(channel_id):
    try:
        video_ids = get_video_ids(channel_id)        
        video_details = get_video_details(video_ids)        
        for video_detail in video_details:
            video_detail = (video_detail['video_id'],video_detail['channel_id'],video_detail['video_name'],video_detail['video_description'],video_detail['published_date'],video_detail['view_count'],video_detail['like_count'],video_detail['favorite_count'],video_detail['comment_count'],video_detail['duration'],video_detail['thumbnail'],video_detail['caption_status'])
            insert_query = '''INSERT INTO Video 
            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
            mycursor.execute(insert_query,video_detail)
            conn.commit()
    except Exception as err:
        print(err)        
        
def insert_comment_details(channel_id):
    try:        
        video_ids = get_video_ids(channel_id)
        comment_details = get_comment_details(video_ids)
        for comment_detail in comment_details:
            date_string = comment_detail['comment_published_date'] 
            date_string = changeDateFormat(date_string)
            comment_detail['comment_published_date'] = date_string
            comment_detail = (comment_detail['comment_id'],comment_detail['video_id'],comment_detail['comment_text'],comment_detail['comment_author'],comment_detail['comment_published_date'])
            insert_query = '''INSERT INTO Comment 
            VALUES(%s,%s,%s,%s,%s)''' 
            mycursor.execute(insert_query,comment_detail)
            conn.commit()
    except Exception as err:
        print(err) 
        pass


def insert_playlist_details(playlist_data):
    try:
        insert_query = '''INSERT IGNORE INTO Playlist (Playlist_Id, Title, Channel_Id, Channel_Name, PublishedAt, Video_Count)
                          VALUES (%s, %s, %s, %s, %s, %s)'''
        for playlist_item in playlist_data:
            published_at = datetime.strptime(playlist_item['PublishedAt'], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            mycursor.execute(insert_query, (
                playlist_item['Playlist_Id'],
                playlist_item['Title'],
                playlist_item['Channel_Id'],
                playlist_item['Channel_Name'],
                published_at,  
                playlist_item['Video_Count']
            ))
        conn.commit()
    except mysql.connector.Error as err:
        if err.errno == 1062:
            print("Duplicate entry. Skipping...")
        else:
            print("Error:", err)
    except Exception as err:
        print("Error:", err)

# Streamlit Part
st.set_page_config(page_title='Youtube Harvest', page_icon= 'card_file_box', layout= 'wide')

st.markdown("<h1 style='color: Black;'>Welcome to YouTube Harvesting project!!!</h1>", unsafe_allow_html=True)


left, middle_one, middle_two, right = st.columns(4)

# Page content
if left.button("Home", use_container_width=True):
    st.write("""
             YouTube data harvesting and warehousing using Python and Streamlit involves using the YouTube Data API to fetch data like video details, channel information, comments, and playlists. This data is then processed, cleaned, and transformed before being stored in a MySQL database using Mysql-connector. Display it in streamlit web applications After fetching data in mysql database.
             """)



if middle_one.button("Extract & Add Data Into Database", use_container_width=True):
    st.subheader("Provide Channel ID")
    channel_id = st.text_input("Channel ID")
    
    if st.button("Collect & Store Channel data"):
        st.write('running!') 
        with st.spinner("Collecting data and storing in the database..."):
            try:
                insert_all_table(channel_id)
                st.success('Uploaded Successfully!')
            except Exception as e:
                st.error(f"An error occurred: {e}")
        

            
if middle_two.button("View Tables", icon="😃", use_container_width=True):
    st.subheader("Select the table to be viewed from SQL Database")
    view_table = st.selectbox("Select the table to view from MySql", ["Channels", "Videos", "Comments","Playlist"])
    if view_table == "Channels":
        show_channel_table()
    elif view_table == "Videos":
        show_video_table()
    elif view_table == "Comments":
        show_comment_table()
    elif view_table == "Playlist":
        show_playlist_table()



@st.cache_data
def run_query(query):
    mycursor.execute(query)
    return pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)

if right.button("SQL Query", icon=":material/mood:", use_container_width=True):
    st.session_state['show_queries'] = True

    if 'show_queries' in st.session_state:
        st.subheader("Please select a query to execute")
        questions = st.selectbox('Select Option',
                                ['1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])
                

        if questions == '1. What are the names of all the videos and their corresponding channels?':
            query = '''SELECT Video.video_name AS Video_Title, Channel.channel_name AS Channel_Name 
                    FROM Video LEFT JOIN Channel ON Video.channel_id = Channel.channel_id;'''
            df = run_query(query)
            st.write(df)

        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            query = """SELECT Channel.channel_name AS Channel_Name, COUNT(Video.video_id) AS Video_Count 
                    FROM Video RIGHT JOIN Channel ON Video.channel_id = Channel.channel_id
                    GROUP BY channel.channel_id ORDER BY Video_Count DESC;"""
            df = run_query(query)
            st.write(df)

        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            query= """SELECT  Channel.channel_name Channel_Name, Video.view_count View_Count, Video.video_name Video_Name FROM Video
                                RIGHT JOIN Channel
                                ON Video.channel_id = Channel.channel_id
                                ORDER BY view_count DESC
                                LIMIT 10;"""
            df = run_query(query)
            st.write(df)

        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            query= """SELECT Video.video_id Video_Id, video.video_name Video_Name,COUNT(comment_id) AS Comment_Count FROM Comment
                                LEFT JOIN Video
                                ON Comment.video_id = Video.video_id
                                GROUP BY Video.video_id
                                ORDER BY comment_count DESC;"""
            df = run_query(query)
            st.write(df)

        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            query= """SELECT  Channel.channel_name Channel_Name, Video.like_count Likes, Video.video_name Video_Name FROM Video
                                RIGHT JOIN Channel
                                ON Video.channel_id = Channel.channel_id
                                ORDER BY like_count DESC;"""
            df = run_query(query)
            st.write(df)
        
        elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
            query= """SELECT video_name Video_Name, like_count Likes FROM Video
                                ORDER BY like_count DESC;"""
            df = run_query(query)
            st.write(df)

        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            query= """SELECT channel_name Channel_Name, channel_views AS View_Count
                                FROM Channel
                                ORDER BY view_count DESC;"""
            df = run_query(query)
            st.write(df)

        elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            query= """SELECT  Channel.channel_name Channel_Name,Video.video_name Video_Name ,Video.published_date Published_Date FROM Video 
                                LEFT JOIN Channel 
                                ON Video.channel_id = Channel.channel_id
                                WHERE Video.published_date LIKE '2022%';
                                """
            df = run_query(query)
            st.write(df)
        
        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            query= """SELECT Channel.channel_name Channel_Name ,ROUND(AVG(Video.duration)/ 60,2) as 'Duration in Minutes' FROM Video
                                RIGHT JOIN Channel
                                ON Video.channel_id = Channel.channel_id
                                GROUP BY channel.channel_id
                                ORDER BY `Duration in Minutes` DESC;
                                """
            df = run_query(query)
            st.write(df)
        
        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            query= """SELECT Channel.channel_name Channel_Name,video.video_name Video_Name, COUNT(comment_id) AS Comment_Count FROM Comment
                                Left JOIN Video ON Comment.video_id = Video.video_id
                                inner JOIN CHANNEL ON Video.channel_id = Channel.channel_id
                                GROUP BY Video.video_id
                                ORDER BY comment_count DESC
                                """
            df = run_query(query)
            st.write(df)

