import pandas as pd
import pymongo
import mysql.connector
import re
import datetime
import streamlit as st
from streamlit_option_menu import option_menu
from googleapiclient.discovery import build

def webservice():
    Api_key = "AIzaSyAyOBpF0I01siyxbp8d9Ok9FtpqhmwoI_M"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=Api_key)
    return youtube

youtube = webservice()

def Channel(channel_id):
    all_data =[]
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

#print(response)

    for item in response["items"]:
        data={
            "Channel_Name":item["snippet"]["title"],
            "Channel_Id":item["id"],
            "Subscription_Count":item["statistics"]["subscriberCount"],
            "Channel_Views":item["statistics"]["viewCount"],
            "Total_Videos":item["statistics"]["videoCount"],
            "Playlist_id":item["contentDetails"]["relatedPlaylists"]["uploads"],
            "Channel_description":item["snippet"]["description"]
        }
        all_data.append(data)
        
    return all_data

#Channel('UC4gJntprzE2tf501BkxMMQA')

def Playlist(channel_id):
    playlist_data = []
    next_page_token = None

    while True:
        request = youtube.playlists().list(
            part       ="snippet,contentDetails",
            channelId  =channel_id,
            maxResults =50,
            pageToken  =next_page_token
        )
        response = request.execute()

        for item in response["items"]: 
            data={
                "PlaylistId"  :item["id"],
                "Title"       :item["snippet"]["title"],
                "ChannelId"   :item["snippet"]["channelId"],
                "Channel_Name":item["snippet"]["channelTitle"],
                "PublishedAt" :item["snippet"]["publishedAt"],
                "VideoCount"  :item["contentDetails"]["itemCount"]
            }
            playlist_data.append(data)

        next_page_token = response.get("nextPageToken")
        if next_page_token is None:
            break

    return playlist_data

#Playlist("UC4gJntprzE2tf501BkxMMQA")

def Videos_id(channel_id):
    videos_id=[]

    
    response = youtube.channels().list(
            id   = channel_id,
            part  ="contentDetails").execute()
        
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    
    next_page_token = None

    while True:
        response1 = youtube.playlistItems().list(
            part       ="contentDetails",
            playlistId =playlist_Id,
            maxResults =50,
            pageToken  =next_page_token).execute()
        
        for i in response1["items"]:
            videos_id.append(i["contentDetails"]["videoId"])
        next_page_token = response1.get("nextPageToken")
    

        if next_page_token is None :
            break


    return videos_id

#Videos_id("UCVGETS6YDIf7C0VVY1BeC3w")

def video_info(videos_id):
    video_data = []
    for Video_id in videos_id:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id  =Video_id,
        )
        response=request.execute()

        for item in response["items"]:
            data ={
                "Video_Id"      : item["id"],
                "Channel_Name"  : item["snippet"]["channelTitle"],
                "Channel_Id"    : item["snippet"]["channelId"],
                "Title"         : item["snippet"]["title"],
                "Thumbnail"     : item["snippet"]["thumbnails"]["default"]["url"],
                "description"   : item["snippet"]["description"],
                "PublishedAt"   : item["snippet"]["publishedAt"],
                "Duration"      : item["contentDetails"]["duration"],
                "Definition"    : item["contentDetails"]["definition"],
                "View_Count"    : item["statistics"].get("viewCount"),
                "Like_Count"    : item["statistics"].get("likeCount"),
                "Comment_count" : item["statistics"].get("commentCount"),
                "Favorite_Count": item["statistics"].get("favoriteCount"),
                "Caption_Status": item["contentDetails"]["caption"]
                }
            video_data.append(data)
       
    return video_data

#pd.DataFrame(video_info(["lV86Kf6DD7k"]))

def Comments(videos_id):
    comment_data=[]
    
    for video_id in videos_id:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=50
        )
        response=request.execute()

        for item in response["items"]:
            data = {
                'Comment_Id': item["snippet"]["topLevelComment"]["id"],
                'Video_Id': item["snippet"]["videoId"],
                'Comment_Text': item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                'Comment_Author': item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                'Comment_PublishedAt': item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            }
            comment_data.append(data)
            
    return comment_data

from pymongo.mongo_client import MongoClient
client = MongoClient("mongodb+srv://divya:1234@cluster0.afrzrns.mongodb.net/")
database = client.youtube

def channel_details(channel_id):
    ch_details = Channel(channel_id)
    pl_details = Playlist(channel_id)
    vid_ids    = Videos_id(channel_id)
    video_dt   = video_info(vid_ids)
    comment_dt = Comments(vid_ids)

    collection1 = database["channel_details"]
    collection1.insert_one({
                            "channel_information" :ch_details,
                            "playlist_information":pl_details,
                            "video_information"   :video_dt,
                            "comment_information" :comment_dt
                            })
    return "uploaded successfully"
#channel_details("UCqf-C-dxAFNWKYZG61PYn8A")

mydb = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
 )
print(mydb)
mycursor = mydb.cursor(buffered=True)
#mycursor.execute('CREATE DATABASE youtube')

def channels_table():
    mydb = mysql.connector.connect(                            
    host="localhost",
    user="root",
    password="",
    database="youtube",
    port=3306
    )

    mycursor=mydb.cursor(buffered=True)

    drop_query = "DROP TABLE IF EXISTS channels"
    mycursor.execute(drop_query)
    mydb.commit()
    try:
        create_query ='''CREATE TABLE IF NOT EXISTS channels(Channel_Name varchar(255),
                                                            Channel_Id varchar(255) primary key,
                                                            Subscription_Count int,
                                                            Channel_Views bigint,
                                                            Total_Videos int,
                                                            Playlist_id varchar(255),
                                                            Channel_description text)'''

        mycursor.execute(create_query)
        mydb.commit()
    except Exception as e:
        print(f"Error creating table: {e}")

    channel_list=[]
    database=client["youtube"]
    collection1 =database["channel_details"]

    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        channel_list.extend(ch_data["channel_information"])

    df_channel= pd.DataFrame(channel_list)

    for index,row in df_channel.iterrows():
        insert_query = '''INSERT INTO channels(Channel_Name,
                                            Channel_Id,
                                            Subscription_Count,
                                            Channel_Views,
                                            Total_Videos,
                                            Playlist_id,
                                            Channel_description)
                                            values(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscription_Count'],
                row['Channel_Views'],
                row['Total_Videos'],
                row['Playlist_id'],
                row['Channel_description'])
        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")
        except Exception as e:
            print(f"Error inserting data: {e}")

#channels_table()

def playlists_table():
    mydb = mysql.connector.connect(                            
    host="localhost",
    user="root",
    password="",
    database="youtube",
    port=3306
    )
    mycursor=mydb.cursor(buffered=True)

    drop_query = '''DROP TABLE IF EXISTS playlists'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query ='''CREATE TABLE IF NOT EXISTS playlists(PlaylistId varchar(255),
                                                                Title varchar(255),
                                                                ChannelId varchar(255),
                                                                Channel_Name varchar(255),
                                                                PublishedAt timestamp,
                                                                VideoCount int )'''
        
        mycursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating table: {e}")
        
    play_list=[]
    database=client["youtube"]
    collection1 =database["channel_details"]

    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        play_list.extend(pl_data["playlist_information"])
        
    df_playlist= pd.DataFrame(play_list)
    
    for index,row in df_playlist.iterrows():
        published_at_str = row['PublishedAt']
        published_at_obj = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")
        
        insert_query = '''INSERT INTO playlists(PlaylistId,
                                                Title,
                                                ChannelId,
                                                Channel_Name,
                                                PublishedAt,
                                                VideoCount)
                                                values(%s,%s,%s,%s,%s,%s)'''

        values=(row['PlaylistId'],
                row['Title'],
                row['ChannelId'],
                row['Channel_Name'],
                published_at_obj,
                row['VideoCount'])
    
        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")
            
        except Exception as e:
            print(f"Error inserting data: {e}")

#playlists_table()
   
def videos_table():
    mydb = mysql.connector.connect(                            
        host="localhost",
        user="root",
        password="",
        database="youtube",
        port=3306
    )

    mycursor=mydb.cursor(buffered=True)

    drop_query = "DROP TABLE IF EXISTS videos"
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query ='''CREATE TABLE IF NOT EXISTS videos(Video_Id varchar(255) primary key,
                                                            Channel_Name varchar(255),
                                                            Channel_Id varchar(255),
                                                            Title varchar(255),
                                                            Thumbnail varchar(255),
                                                            description text,
                                                            PublishedAt timestamp,
                                                            Duration int,
                                                            Definition varchar(255),
                                                            View_Count bigint,
                                                            Like_Count int,
                                                            Comment_count int,
                                                            Favorite_Count int,
                                                            Caption_Status varchar(255))'''

        mycursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating table: {e}")

        
    video_list=[]
    database=client["youtube"]
    collection1 =database["channel_details"]


    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        video_list.extend(vi_data["video_information"])

    df_video = pd.DataFrame(video_list)
    
    
    for index,row in df_video.iterrows():
        published_at_str = row['PublishedAt']
        published_at_obj = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")
        
        duration_str = row['Duration']
        duration_in_seconds = int(re.findall(r'\d+', duration_str)[0])
        
        insert_query = '''INSERT INTO videos(
                            Video_Id,
                            Channel_Name,
                            Channel_Id,
                            Title,
                            Thumbnail,
                            description,
                            PublishedAt,
                            Duration,
                            Definition,
                            View_Count,
                            Like_Count,
                            Comment_count,
                            Favorite_Count,
                            Caption_Status
                            )
                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Video_Id'],
                row['Channel_Name'],
                row['Channel_Id'],
                row['Title'],
                row['Thumbnail'],
                row['description'],
                published_at_obj,
                duration_in_seconds,
                row['Definition'],
                row['View_Count'],
                row['Like_Count'],
                row['Comment_count'],
                row['Favorite_Count'],
                row['Caption_Status'])
        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")
                
        except Exception as e:
            print(f"Error inserting data: {e}")

#videos_table()

def comments_table():
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="youtube",
        port=3306
    )

    mycursor = mydb.cursor(buffered=True)

    drop_query = "DROP TABLE IF EXISTS comments"
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''CREATE TABLE IF NOT EXISTS comments(
                                                            Comment_Id varchar(255) primary key,
                                                            Video_Id varchar(255),
                                                            Comment_Text text,
                                                            Comment_Author varchar(255),
                                                            Comment_PublishedAt timestamp)'''

        mycursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(f"Error creating table: {e}")

    comment_list = []
    database = client["youtube"]
    collection1 = database["channel_details"]

    for com_data in collection1.find({}, {"_id": 0, "comment_information": 1}):
        comment_list.extend(com_data["comment_information"])

    df_comment = pd.DataFrame(comment_list)

    for index, row in df_comment.iterrows():
        published_at_str = row['Comment_PublishedAt']
        published_at_obj = datetime.datetime.strptime(published_at_str, "%Y-%m-%dT%H:%M:%SZ")

        insert_query = '''INSERT INTO comments(Comment_Id,
            Video_Id,
            Comment_Text,
            Comment_Author,
            Comment_PublishedAt)
            VALUES (%s, %s, %s, %s, %s)'''

        values = (
            row['Comment_Id'],
            row['Video_Id'],
            row['Comment_Text'],
            row['Comment_Author'],
            published_at_obj
        )

        try:
            mycursor.execute(insert_query, values)
            mydb.commit()
            print("Insertion completed successfully")

        except Exception as e:
            print(f"Error inserting data: {e}")

#comments_table()

mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="youtube",
        port=3306
    )

mycursor = mydb.cursor(buffered=True)

def tables():
    channels_table()
    playlists_table()
    videos_table()
    comments_table()
    
    return "Tables Created Successfully"

#tables()

def Display_channels_table():
    channel_list=[]
    database=client["youtube"]
    collection1 =database["channel_details"]

    for ch_data in collection1.find({},{"_id":0,"channel_information":1}):
        channel_list.extend(ch_data["channel_information"])

    df_channel= st.dataframe(channel_list)
    
    return df_channel

def Display_playlists_table():
    play_list=[]
    database=client["youtube"]
    collection1 =database["channel_details"]

    for pl_data in collection1.find({},{"_id":0,"playlist_information":1}):
        play_list.extend(pl_data["playlist_information"])
        
    df_playlist= st.dataframe(play_list)
    
    return df_playlist

def Display_videos_table():
    video_list=[]
    database=client["youtube"]
    collection1 =database["channel_details"]

    for vi_data in collection1.find({},{"_id":0,"video_information":1}):
        video_list.extend(vi_data["video_information"])

    df_video = st.dataframe(video_list)
    
    return  df_video

def Display_comments_table():
    comment_list=[]
    database = client["youtube"]
    collection1 =database["channel_details"]

    for com_data in collection1.find({},{"_id":0,"comment_information":1}):
        comment_list.extend(com_data["comment_information"])

    df_comment = st.dataframe(comment_list)
    
    return df_comment

#st.write("HELLO WORLD")
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing",
                   layout= "wide",
                   initial_sidebar_state= "expanded"
                   )
# TITLE
st.markdown("<h1 style='text-align: center; color: red;'>Youtube Data Harvesting and Warehousing</h1>", unsafe_allow_html=True)
selected = option_menu(None, ["Home","Upload & Load","Questions"], 
                       icons=["house","cloud-upload","question"],
                       default_index=0,
                       orientation="horizontal",
                       styles={"nav-link": {"font-size": "30px", "text-align": "centre", "margin": "-2px", "--hover-color": "#6495ED"},
                               "icon": {"font-size": "35px"},
                               "container" : {"max-width": "6000px"},
                               "nav-link-selected": {"background-color": "#6495ED"}})

if selected == "Home":
    st.divider()

    st.markdown("""
                This Project is about Youtube Data Harvesting and Warehousing using SQL, MONGODB, STREAMLIT.
    This project aims to demonstrate how to collect, store, and interact with YouTube data using popular database technologies and a powerful Python framework.

    **Basic need:**
    - Understanding of Python programming.
    - Basics of API
    - Knowledge of SQL and MongoDB databases.
    - Familiarity with the basics of web development using Streamlit.

    **Tools and Technologies:**
    - Python
    - SQL(MySQL) 
    - MongoDB
    - Streamlit
    """)


if selected == "Upload & Load":
    st.title("Upload and Load Data")
    channel_id = st.text_input("#### Enter Youtube Channel_ID below:")

    if st.button("Collect and Store Data"):
        ch_ids = []
        database = client["youtube"]
        collection1 = database["channel_details"]

        for ch_data in collection1.find({}, {"_id": 0, "channel_information": 1}):
            for channel_info in ch_data["channel_information"]:
                ch_ids.append(channel_info["Channel_Id"])

        if channel_id in ch_ids:
            st.success("Channel details of the given channel id: " + channel_id + " already exist")
        else:
            output = channel_details(channel_id)
            st.success(output)

    if st.button("Migrate to SQL"):
        display = tables()
        st.success(display)

    Display_table = st.radio("### SELECT THE TABLE FOR VIEWING:",
                            (":black[CHANNELS]", 
                             ":black[PLAYLISTS]", 
                             ":black[VIDEOS]", 
                             ":black[COMMENTS]"), index=None)
    
    st.write("**You selected:**", Display_table)

    if Display_table == ":black[CHANNELS]":
        Display_channels_table()
    elif Display_table == ":black[PLAYLISTS]":
        Display_playlists_table()
    elif Display_table == ":black[VIDEOS]":
        Display_videos_table()
    elif Display_table == ":black[COMMENTS]":
        Display_comments_table()
        

if selected == "Questions":
    st.title("Select any questions to get Insights")
    
#MySQL connection and question selection code here
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="youtube",
        port=3306
    )
    mycursor = mydb.cursor()

    question = st.selectbox('**Questions**',
                            ('1. Names of all the Videos and their Channel?',
                             '2. Channels with most No.of.Videos and display the counts?',
                             '3. Top 10 most viewed videos and their Channels?',
                             '4. Comments in each video?',
                             '5. Videos with highest likes?',
                             '6. Likes of all videos?',
                             '7. Views of each channel?',
                             '8. Videos published in the year 2022?',
                             '9. Average duration of all videos in each channel?',
                             '10. Videos with the highest number of comments?'))

    

    if question == '1. Names of all the Videos and their Channel?':
        query1 = '''select Title as videos, Channel_Name as ChannelName from videos'''
        mycursor.execute(query1)
        t1 = mycursor.fetchall()
        st.write(pd.DataFrame(t1, columns=["Video Title", "Channel Name"]))

    elif question == '2. Channels with most No.of.Videos and display the counts?':
        query2 = '''select Channel_Name as ChannelName,Total_Videos as No_of_Videos from channels 
                        order by No_of_Videos desc'''
        mycursor.execute(query2)
        t2=mycursor.fetchall()
        st.write(pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"]))
        df_question2 = pd.DataFrame(t2, columns=["Channel Name", "No Of Videos"])
        st.bar_chart(df_question2.set_index("Channel Name"))

        
    elif question == '3. Top 10 most viewed videos and their Channels?':
        query3 = '''select Title as VideoTitle, Channel_Name as ChannelName, View_count as Views from videos
                        order by Views desc limit 10'''
        mycursor.execute(query3)
        t3 = mycursor.fetchall()
        st.write(pd.DataFrame(t3, columns=["Video Title", "Channel Name", "Views"]))
        
    elif question == '4. Comments in each video?':
        query4 = '''select Title as VideoTitle,Channel_Name as ChannelName,Comment_count as No_comments from videos 
                        where Comment_count is  not null order by no_comments desc'''
        mycursor.execute(query4)
        t4 = mycursor.fetchall()
        st.write(pd.DataFrame(t4, columns=["Video Title", "Channel Name", "Comments"]))
        
    elif question == '5. Videos with highest likes?':
        query5 = '''select Title as VideoTitle, Channel_Name as ChannelName, Like_Count as Likes from videos
                        order by Likes desc limit 10'''
        mycursor.execute(query5)
        t5 = mycursor.fetchall()
        st.write(pd.DataFrame(t5, columns=["Video Title", "Channel Name", "Likes"]))
        
    elif question == '6. Likes of all videos?':
        query6 = '''select Title as VideoTitle, Channel_Name as ChannelName, Like_count as Likes from videos'''
        mycursor.execute(query6)
        t6 = mycursor.fetchall()
        st.write(pd.DataFrame(t6, columns=["Video Title", "Channel Name", "Likes"]))
        
    elif question == '7. Views of each channel?':
        query7 = '''select Channel_Name as ChannelName, Channel_Views as TotalViews from channels 
                        order by TotalViews desc '''
        mycursor.execute(query7)
        t7 = mycursor.fetchall()
        st.write(pd.DataFrame(t7, columns=["Channel Name", "Total Views"]))
        
    elif question == '8. Videos published in the year 2022?':
        query8 = '''select Title as VideoTitle, Channel_Name as ChannelName, PublishedAt as PublishedDT from videos
                        where year(PublishedAt) = 2022'''
        mycursor.execute(query8)
        t8 = mycursor.fetchall()
        st.write(pd.DataFrame(t8, columns=["Video Title", "Channel Name", "Published D&T"]))
                
    elif question == '9. Average duration of all videos in each channel?':
        query9 = '''select Channel_Name as ChannelName, avg(Duration) as AverageDuration from videos
                        group by Channel_Name'''
        mycursor.execute(query9)
        t9 = mycursor.fetchall()
        st.write(pd.DataFrame(t9, columns=["Channel Name", "Average Duration"]))
        
    elif question == '10. Videos with the highest number of comments?':
        query10 = '''select Title as VideoTitle, Channel_Name as ChannelName, Comment_count as comments from videos 
                        order by Comments desc limit 10'''
        mycursor.execute(query10)
        t10 = mycursor.fetchall()
        st.write(pd.DataFrame(t10, columns=["Video Title", "Channel Name", "Comments"]))
