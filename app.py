import googleapiclient.discovery
import streamlit as st
import pymongo as pymg
import psycopg2 as psyc
import pandas as pd
from PIL import Image
from streamlit_option_menu import option_menu
import plotly.express as px

class youtube_extraction:

    @staticmethod
    def api_connection():
        api_service_name = "youtube"
        api_version = "v3"
        api_key='AIzaSyBE7itdaejiafXzh3gKsI8xeetJ9qTbZwA'
        youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
        return youtube    
    
    youtube=api_connection()


    def get_channel_details(c_id):
        request=youtube_extraction.youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=c_id)
        response = request.execute()
        for i in response['items']:
                data = {"Channel_Id":i["id"],
                        "Channel_Name":i['snippet']['title'],
                        "Channel_Description":i['snippet']['description'],
                        "PublishedAt":i['snippet']['publishedAt'],
                        "Playlist_Id":i['contentDetails']['relatedPlaylists']['uploads'],
                        "Subscribers":i['statistics']['subscriberCount'],
                        "Channel_views":i['statistics']['viewCount'],
                        "Total_Videos":i['statistics']['videoCount']
                        }
        return data 


    def get_playlist_details(channel_info):
        playlist_data=[]
        pl_c_id=channel_info['Channel_Id']
        next_page_token=None
        while True:
            request=youtube_extraction.youtube.playlists().list(part="snippet,contentDetails",channelId=pl_c_id,maxResults=50, pageToken=next_page_token)
            response=request.execute()
            for i in response['items']:
                data={"Playlist_Id":i['id'],
                    "Title":i['snippet']['title'],
                    "Channel_Id":i['snippet']['channelId'],
                    "Channel_Name":i['snippet']['channelTitle'],
                    "PublishedAt":i['snippet']['publishedAt'],
                    "Video_Count":i['contentDetails']['itemCount']}
                playlist_data.append(data)
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                break
        return playlist_data

    #for a given channel id,fetch the Playlist_ID of all videos and video_ID for each video
    def get_video_ids(channel_info):
        video_ids=[]
        v_plid=channel_info['Playlist_Id']
        next_page_token=None
        while True:
            request=youtube_extraction.youtube.playlistItems().list(
                part="snippet,contentDetails", playlistId = v_plid,maxResults=50,pageToken=next_page_token)
            response=request.execute()
            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
            next_page_token=response.get('nextPageToken')
            if next_page_token is None:
                break
        return video_ids


    def get_video_details(video_ids):
        video_content=[]

        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b
        
        for i in video_ids:
            request=youtube_extraction.youtube.videos().list(part="snippet,contentDetails,statistics",id=i)
            response = request.execute()
            for j in response['items']:
                data = {"Video_Id":j['id'],
                        "Channel_Id":j['snippet']['channelId'],
                        "Title":j['snippet']['title'],
                        "Channel_Name":j['snippet']['channelTitle'],
                        "Description":j['snippet']['description'],
                        "PublishedAt":j['snippet']['publishedAt'],
                        "Tags":j['snippet'].get('tags'),
                        "Thumbnail":j['snippet']['thumbnails']['default']['url'],
                        "Duration":time_duration(j['contentDetails']['duration']),
                        "contentRating":j['contentDetails']['contentRating'],
                        "Caption":j['contentDetails']['caption'],
                        "licensedContent":j['contentDetails']['licensedContent'],
                        "Definition":j['contentDetails']['definition'],
                        "View_count":j['statistics'].get('viewCount'),
                        "Like_count":j['statistics'].get('likeCount'),
                        "Comment_count":j['statistics'].get('commentCount'),
                        "Fav_count":j['statistics'].get('favoriteCount')
                        }
                video_content.append(data)
        return video_content


    def get_comment_details(video_ids):
        comment_content=[]
        try:
            for i in video_ids:
                request=youtube_extraction.youtube.commentThreads().list(part="snippet",videoId=i,maxResults=50)
                response=request.execute()
                for j in response['items']:
                    data= {"Comment_Id":j['snippet']['topLevelComment']['id'],
                            "Video_Id":j['snippet']['topLevelComment']['snippet']['videoId'],
                            "Comment_Text":j['snippet']['topLevelComment']['snippet']['textDisplay'],
                            "Comment_Author":j['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            "Comment_Published":j['snippet']['topLevelComment']['snippet']['publishedAt'] }
                    comment_content.append(data)
        except:
            pass
        return comment_content


    def channel_content(Channel_ID):
        youtube_dict = dict()
        youtube_dict['channel_info']=youtube_extraction.get_channel_details(Channel_ID)
        youtube_dict['playlist_info']=youtube_extraction.get_playlist_details( youtube_dict['channel_info'])
        youtube_dict['all_video_ids']=youtube_extraction.get_video_ids( youtube_dict['channel_info'])
        youtube_dict['video_info']=youtube_extraction.get_video_details(youtube_dict['all_video_ids'])
        youtube_dict['comment_info']=youtube_extraction.get_comment_details(youtube_dict['all_video_ids'])
        return youtube_dict

class mongo_operations:
    
    @staticmethod
    def mongoDB_collection():
        client=pymg.MongoClient("mongodb+srv://maheshwariswaminathan:ocJ5zarf3JSDRQu3@cluster0.gf3ephp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
        db=client["Youtube_Content"]
        return db["channel_content"]

    collection=mongoDB_collection()
    
    def load_mongoDB(Channel_Id, youtube_dict):
        mongo_operations.collection.delete_many({'channel_information.Channel_Id' : Channel_Id})
        mongo_operations.collection.insert_one({"channel_information":youtube_dict['channel_info'],
                                            "playlist_information":youtube_dict['playlist_info'],
                                            "all_video_ids":youtube_dict['all_video_ids'],
                                            "video_information":youtube_dict['video_info'],
                                            "comment_information":youtube_dict['comment_info']})
        return "Uploaded information in the mongoDB successfully"
    
    def get_channel_document(Channel_ID):
        ch_list=[]
        for ch_data in mongo_operations.collection.find({'channel_information.Channel_Id' : Channel_ID},
                                                    {"_id":0,"channel_information":1}):
            ch_list.append(ch_data["channel_information"])    
        df_channel_info=pd.DataFrame(ch_list)
        return df_channel_info
    
    def get_playlist_document(Channel_ID):
        pl_list=[]
        for pl_data in mongo_operations.collection.find({'channel_information.Channel_Id' : Channel_ID}
                                                    ,{"_id":0,"playlist_information":1}):
            for i in range(len(pl_data["playlist_information"])):
                pl_list.append(pl_data["playlist_information"][i])
        df_playlist_info=pd.DataFrame(pl_list)
        return df_playlist_info
    
    def get_video_document(Channel_ID):
        vi_list=[]
        for vi_data in mongo_operations.collection.find({'channel_information.Channel_Id' : Channel_ID},
                                                    {"_id":0,"video_information":1}):
            for i in range(len(vi_data["video_information"])):
                vi_list.append(vi_data["video_information"][i])
        df_video_inf=pd.DataFrame(vi_list)
        return df_video_inf
    
    def get_channel_comments_document(Channel_ID):
        cm_list=[]
        for cm_data in mongo_operations.collection.find({'channel_information.Channel_Id' : Channel_ID},
                                                    {"_id":0,"comment_information":1}):
            for i in range(len(cm_data["comment_information"])):
                cm_list.append(cm_data["comment_information"][i])
        df_comment_info=pd.DataFrame(cm_list)
        return df_comment_info


class postgres_operations:

    @staticmethod
    def postgresSql_connection():
        mydb=psyc.connect(host="localhost",
                            user="postgres",
                            password="maksi@2024",
                            database="Youtube_Content",
                            port="5432")
        return mydb

    mydb=postgresSql_connection()
    cursor=mydb.cursor()

    def load_into_db(Channel_ID):
        df_channel_info=mongo_operations.get_channel_document(Channel_ID)
        postgres_operations.load_channelData(df_channel_info)
        df_playlist_info=mongo_operations.get_playlist_document(Channel_ID)
        postgres_operations.load_playlistData(df_playlist_info)
        df_video_inf=mongo_operations.get_video_document(Channel_ID)
        postgres_operations.load_videoData(df_video_inf)
        df_comment_info=mongo_operations.get_channel_comments_document(Channel_ID)
        postgres_operations.load_commentData(df_comment_info)
        return 'Data loaded into PostgreSQL'

    def load_channelData(df_channel_info):
        for index,row in df_channel_info.iterrows():
            insert_sql='''insert into channels(Channel_Name,
                                            Channel_Id,
                                            Subscribers,
                                            Channel_Views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_Id)
            
                                            values(%s,%s,%s,%s,%s,%s,%s)
                                            on conflict (Channel_Id)
                                            do update set Channel_Name = EXCLUDED.Channel_Name,
                                            Subscribers = EXCLUDED.Subscribers,
                                            Channel_Views = EXCLUDED.Channel_Views,
                                            Total_Videos = EXCLUDED.Total_Videos,
                                            Channel_Description = EXCLUDED.Channel_Description,
                                            Playlist_Id = EXCLUDED.Playlist_Id'''

            values=(row["Channel_Name"],
                    row["Channel_Id"],
                    row["Subscribers"],
                    row["Channel_views"],
                    row["Total_Videos"],
                    row["Channel_Description"],
                    row["Playlist_Id"])  
            try:
                postgres_operations.cursor.execute(insert_sql,values)      
                postgres_operations.mydb.commit()
            except Exception as error:
                print("Error occured during insertion",error)
                postgres_operations.mydb.rollback()

    def load_playlistData(df_playlist_info):
        for index,row in df_playlist_info.iterrows():
            insert_sql='''insert into playlist(Playlist_Id,
                                                Title,
                                                Channel_Id,
                                                Channel_Name,
                                                PublishedAt,
                                                Total_Videos)
            
                                            values(%s,%s,%s,%s,%s,%s)
                                            on conflict (Playlist_Id)
                                            do update set Title = EXCLUDED.Title,
                                            Channel_Id = EXCLUDED.Channel_Id,
                                            Channel_Name = EXCLUDED.Channel_Name,
                                            PublishedAt = EXCLUDED.PublishedAt,
                                            Total_Videos = EXCLUDED.Total_Videos'''

            values=(row["Playlist_Id"],
                    row["Title"],
                    row["Channel_Id"],
                    row["Channel_Name"],
                    row["PublishedAt"],
                    row["Video_Count"]
                    )  
            try:
                postgres_operations.cursor.execute(insert_sql,values)      
                postgres_operations.mydb.commit()
            except Exception as error:
                print("Error occured during insertion",error)
                postgres_operations.mydb.rollback()

    def load_videoData(df_video_inf):
        for index,row in df_video_inf.iterrows():
            insert_sql='''insert into videos(Video_Id,
                                        Channel_Id,
                                        Title,            
                                        Channel_Name,
                                        Description,
                                        PublishedAt,
                                        Tags,
                                        Thumbnail,
                                        Duration,
                                        Caption,
                                        Definition,
                                        View_count,
                                        Like_count,
                                        Comment_count,
                                        Fav_count )
                                    
                                    values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                                    on conflict (Video_Id)
                                    do update set Channel_Id = EXCLUDED.Channel_Id,
                                    Title = EXCLUDED.Title,
                                    Channel_Name = EXCLUDED.Channel_Name,
                                    Description = EXCLUDED.Description,
                                    PublishedAt = EXCLUDED.PublishedAt,
                                    Tags = EXCLUDED.Tags,
                                    Thumbnail = EXCLUDED.Thumbnail,
                                    Duration = EXCLUDED.Duration,
                                    Caption = EXCLUDED.Caption,
                                    Definition = EXCLUDED.Definition,
                                    View_Count = EXCLUDED.View_count,
                                    Like_Count = EXCLUDED.Like_count,
                                    Comment_Count = EXCLUDED.Comment_count,
                                    Fav_Count = EXCLUDED.Fav_count '''
                
            values=(row["Video_Id"],
                    row["Channel_Id"],
                    row["Title"],            
                    row["Channel_Name"],
                    row["Description"],
                    row["PublishedAt"],
                    row["Tags"],
                    row["Thumbnail"],
                    row["Duration"],
                    row["Caption"],
                    row["Definition"],
                    row["View_count"],
                    row["Like_count"],
                    row["Comment_count"],
                    row["Fav_count"]
                    )
            try:
                postgres_operations.cursor.execute(insert_sql,values)
                postgres_operations.mydb.commit()
            except Exception as error:
                print("Video values are already inserted",error)
                postgres_operations.mydb.rollback()

    def load_commentData(df_comment_info):
        for index,row in df_comment_info.iterrows():
            insert_sql='''insert into Comments_tbl(Comment_Id,
                                            Video_Id,
                                            Comment_Text,
                                            Comment_Author,
                                            PublishedAt )
                                    
                                            values(%s,%s,%s,%s,%s)
                                            on conflict (Comment_Id)
                                            do update set Video_Id = EXCLUDED.Video_Id,
                                            Comment_Text = EXCLUDED.Comment_Text,
                                            Comment_Author = EXCLUDED.Comment_Author,
                                            PublishedAt = EXCLUDED.PublishedAt '''

            values=(row["Comment_Id"],
                    row["Video_Id"],
                    row["Comment_Text"],            
                    row["Comment_Author"],
                    row["Comment_Published"]
                    )
            try:
                postgres_operations.cursor.execute(insert_sql,values)
                postgres_operations.mydb.commit()
            except Exception as error:
                print("Comment values are already inserted",error)   
                postgres_operations.mydb.rollback()  

    def get_df_from_query(select_query, column_names):
        postgres_operations.cursor.execute(select_query)
        return pd.DataFrame(postgres_operations.cursor.fetchall(),columns=column_names)

#STREAMLIT PART
st.set_page_config(page_title= "Youtube Data Harvesting and Warehousing")

# CREATING OPTION MENU

with st.sidebar:
    selected = option_menu(None, ["Home","Extract Transform and Load","Insights"], 
                           icons=["house-door","database-add","bar-chart"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "18px", "text-align": "centre", "margin": "0px", 
                                                "--hover-color": "0071e3"},
                                   "icon": {"font-size": "18px"},
                                   "container" : {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "0071e3"}})
# HOME PAGE
if selected == "Home": 
    layout= "wide"
    col1,col2 = st.columns(2,gap= 'small')
    col1.markdown("###### :blue[Project Title] : YouTube Data Harvesting and Warehousing using SQL and Streamlit")
    col1.markdown("###### :blue[Domain] : Social Media")
    col1.markdown("###### :blue[Technologies used] : Youtube Data API, Python, MongoDB,Data management using PostgreSQL, Streamlit")
    col2.markdown("######   ")
    col2.markdown("######   ")
    col2.markdown("######   ")

if selected == "Extract Transform and Load":
    if "btnval" not in st.session_state: st.session_state.btnval = False
    def toggle_btns(): 
        st.session_state.btnval = not st.session_state.btnval
    st.markdown("#    ")
    st.write("### Enter a YouTube Channel_ID below :")
    ch_id = st.text_input("Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
    if ch_id and st.button("Extract Data"):
        ch_details = youtube_extraction.channel_content(ch_id)
        st.session_state.ch_details = ch_details
        st.session_state.msg = 'Extracted data from YouTube'
    if 'ch_details' in st.session_state:
        st.write(f'###### About the Channel:')
        st.table(st.session_state.ch_details['channel_info'])
        if st.session_state.msg == 'Extracted data from YouTube':
            if 'ch_details' in st.session_state and st.button("Upload to MongoDB", on_click=toggle_btns, 
                                                            disabled=st.session_state.btnval):
                with st.spinner('Uploading...'):
                    msg=mongo_operations.load_mongoDB(ch_id[0], st.session_state.ch_details)
                    st.session_state.msg = msg
        else:
            st.button("Upload to MongoDB", disabled=True)
    if 'msg' in st.session_state and st.session_state.msg == 'Uploaded information in the mongoDB successfully':
        st.write(f'###### Data loaded into MongoDB successfully')
    if 'msg' in st.session_state and st.session_state.msg == 'Uploaded information in the mongoDB successfully':
        if 'ch_details' in st.session_state and st.button("Proceed to Upload to PostgreSQL", on_click=toggle_btns,
                                                                                        disabled=not st.session_state.btnval):
            with st.spinner('Uploading...'):
                try:
                    postgresmsg=postgres_operations.load_into_db(ch_id[0])
                    st.session_state.msg = postgresmsg
                except:
                    st.error("Error while loading to PostgreSQL!")
    if 'msg' in st.session_state and st.session_state.msg == 'Data loaded into PostgreSQL':
        st.write(f'###### Data loaded into PostgreSQL successfully')

# VIEW PAGE
if selected == "Insights":
    st.write("#### :red[Select any of the below questions to get Insights]")
    questions = st.selectbox('Questions',
    ['Click any of the below queries',
    '1. What are the names of all the videos and their corresponding channels?',
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
        select_statement="""SELECT title AS Video_Title, channel_name AS Channel_Name FROM videos ORDER BY channel_name"""
        st.write(postgres_operations.get_df_from_query(select_statement, ["Video_Title","Channel_Name"]))
        
    elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
        select_statement="""SELECT channel_name 
                            AS Channel_Name, total_videos AS Total_Videos
                            FROM channels
                            ORDER BY total_videos DESC"""
        df = postgres_operations.get_df_from_query(select_statement,["Channel_Name","Total_Videos"])
        st.write(df)
        st.write("###### :blue[Number of videos in each channel :]")
        #st.bar_chart(df, x = df.columns[0], y = df.columns[1])
        fig = px.bar(df,  
                     x=df.columns[0],
                     y=df.columns[1],
                     orientation='v',
                     color=df.columns[0]
                    )
        st.plotly_chart(fig,use_container_width=True)

    elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
        select_statement="""SELECT Channel_Name AS Channel_Name, title AS Video_Title, View_count AS Views 
                            FROM videos
                            ORDER BY View_count DESC
                            LIMIT 10"""
        df = postgres_operations.get_df_from_query(select_statement,["Channel_Name","Video_Title","View_count"])
        st.write(df)
        st.write("### :blue[Top 10 most viewed videos :]")
        fig = px.bar(df,
                        x=df.columns[2],
                        y=df.columns[1],
                        orientation='h',
                        color=df.columns[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
        select_statement="""SELECT a.video_id AS Video_id, a.title AS Video_Title, b.Total_Comments
                            FROM videos AS a
                            LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                            FROM comments_tbl GROUP BY video_id) AS b
                            ON a.video_id = b.video_id
                            ORDER BY b.Total_Comments DESC"""
        st.write(postgres_operations.get_df_from_query(select_statement,["Video_id","Video_Title","Total_Comments"]))
            
    elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
        select_statement="""SELECT channel_name AS Channel_Name,title AS Title,Like_count AS Like_Count 
                            FROM videos
                            ORDER BY Like_count DESC
                            LIMIT 10"""
        df = postgres_operations.get_df_from_query(select_statement,["Channel_Name","Title","Like_count"])
        st.write(df)
        st.write("### :blue[Top 10 most liked videos :]")
        fig = px.bar(df,
                        x=df.columns[2],
                        y=df.columns[1],
                        orientation='h',
                        color=df.columns[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
        select_statement="""SELECT title AS Title, Like_count AS Like_Count
                            FROM videos
                            ORDER BY Like_count DESC"""
        st.write(postgres_operations.get_df_from_query(select_statement,["Title","Like_count"]))
            
    elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
        select_statement="""SELECT Channel_name AS Channel_Name, Channel_views AS Views
                            FROM channels
                            ORDER BY Channel_views DESC"""
        df = postgres_operations.get_df_from_query(select_statement,["Channel_Name","Channel_views"])
        st.write(df)
        st.write("### :blue[Channels vs Views :]")
        fig = px.bar(df,
                        x=df.columns[0],
                        y=df.columns[1],
                        orientation='v',
                        color=df.columns[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
        select_statement="""SELECT Channel_name AS Channel_Name
                            FROM videos
                            WHERE date_part('Year',publishedat)=2022
                            GROUP BY Channel_name
                            ORDER BY Channel_name"""
        st.write(postgres_operations.get_df_from_query(select_statement,["Channel_Name"]))
        
    elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
        select_statement="""SELECT channel_name, 
                        avg(duration) AS average_duration
                        FROM videos
                        GROUP BY channel_name"""
        df=postgres_operations.get_df_from_query(select_statement,["Channel_Name","average_duration"])
        st.write(df)
        st.write("### :blue[Average video duration for channels :]")
        fig = px.bar(df,
                        x=df.columns[0],
                        y=df.columns[1],
                        orientation='v',
                        color=df.columns[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
        select_statement="""SELECT channel_name AS Channel_Name,video_id AS Video_ID,Comment_count AS Comments
                            FROM videos
                            ORDER BY Comment_count DESC
                            LIMIT 10"""
        df = postgres_operations.get_df_from_query(select_statement,["Channel_Name","Video_Id","Comments"])
        st.write(df)
        st.write("### :blue[Videos with most comments :]")
        fig = px.bar(df,
                        x=df.columns[1],
                        y=df.columns[2],
                        orientation='v',
                        color=df.columns[0]
                    )
        st.plotly_chart(fig,use_container_width=True)
