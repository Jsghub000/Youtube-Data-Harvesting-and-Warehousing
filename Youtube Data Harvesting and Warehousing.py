from googleapiclient.discovery import build
import pymongo
import psycopg2
import pandas as pd
import streamlit as st


# Connecting To Api Key:
def Api_connect():
    Api_Id = "AIzaSyDXxUGhexlKhArDht1UajEfKjNfKjufc5Q"
    
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name,api_version,developerKey=Api_Id)
    
    return youtube

youtube=Api_connect()


# Channel Information:
def get_channel_info(channel_id):
    
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id)
    
    response = request.execute()

    for i in response["items"]:
        data =dict(
            Channel_Name=i["snippet"]["title"],
            Channel_Id=i["id"],
            Subscribers=i["statistics"]["subscriberCount"],
            Views=i["statistics"]["viewCount"],
            Total_Videos=i["statistics"]["videoCount"],
            Channel_Description=i["snippet"]["description"],
            Playlist_Id=i["contentDetails"]["relatedPlaylists"]["uploads"]
        )
    return data


# Video Ids:
def get_video_ids(channel_id): 
    video_ids=[]
    response= youtube.channels().list(id=channel_id,
                                    part="contentDetails").execute()
    playlist_Id=response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    next_page_token = None
    while True:
        response1=youtube.playlistItems().list(
                                            part ="snippet",
                                            playlistId=playlist_Id,
                                            maxResults=50,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1["items"])):
            video_ids.append(response1["items"][i]["snippet"]["resourceId"]["videoId"])
        next_page_token=response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids        


# Video Information:
def get_video_info(video_ids):

    video_data = []

    for video_id in video_ids:
        request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id= video_id)
        response = request.execute()

        for item in response["items"]:
            data = dict(Channel_Name = item['snippet']['channelTitle'],
                        Channel_Id = item['snippet']['channelId'],
                        Video_Id = item['id'],
                        Title = item['snippet']['title'],
                        Tags = item['snippet'].get('tags'),
                        Thumbnail = item['snippet']['thumbnails']['default']['url'],
                        Description = item['snippet'].get('description'),
                        Published_Date = item['snippet']['publishedAt'],
                        Duration = item['contentDetails']['duration'],
                        Views = item['statistics'].get('viewCount'),
                        Likes = item['statistics'].get('likeCount'),
                        Comments = item['statistics'].get('commentCount'),
                        Favorite_Count = item['statistics']['favoriteCount'],
                        Definition = item['contentDetails']['definition'],
                        Caption_Status = item['contentDetails']['caption']
                        )
            video_data.append(data)
    return video_data


# Comment Information:
def get_comment_info(video_ids):
    comment_data=[]

    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response=request.execute()

            for item in response["items"]:
                data=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"],
                          Video_Id = item["snippet"]["videoId"],
                          Comment_Text = item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                          Comment_Author = item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                          Comment_Published = item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])

                comment_data.append(data)

    except:
        pass
    
    return comment_data


#Getting_Playlist_Details
def get_playlist_details(channel_id):

    All_data=[]

    next_page_token=None
    next_page = True
    while next_page:

        request=youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response=request.execute()

        for item in response['items']: 
                    data=dict(Playlist_Id=item['id'],
                                            Title=item['snippet']['title'],
                                            Channel_Id=item['snippet']['channelId'],
                                            Channel_Name=item['snippet']['channelTitle'],
                                            Published_At=item['snippet']['publishedAt'],
                                            Video_Count=item['contentDetails']['itemCount'])

                    All_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
                next_page=False
                
    return All_data            


#Connecting MongoDB
client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]


#Upload to MongoDB
def Channel_details(channel_id):
    
    channel_details=get_channel_info(channel_id)
    playlist_details=get_playlist_details(channel_id)
    video_ids=get_video_ids(channel_id)
    video_details=get_video_info(video_ids)
    comment_details=get_comment_info(video_ids)

    collection1=db["Channel_details"]
    collection1.insert_one({"Channel_Information":channel_details,
                            "Playlist_Information":playlist_details,
                            "Video_Information":video_details,
                            "Comment_Information":comment_details})
    
    return "Upload Completed Successfully"


#Table Creation For Channels,Playlists,Videos,Comments.

def channels_table(): 
    mydb=psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sqldir",
                          database="youtube_data",
                          port="5432")

    cursor=mydb.cursor()

    drop_query="Drop table if exists channels"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''Create Table If not exists channels(Channel_Name varchar(100),
                                                            Channel_Id varchar(80) primary key,
                                                            Subscribers bigint,
                                                            Views bigint,
                                                            Total_Videos int,
                                                            Channel_Description text,
                                                            Playlist_Id varchar(80))'''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Channels Table Already Created")

    ch_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({},{"_id":0,"Channel_Information":1}):
        ch_list.append(ch_data["Channel_Information"])
    df=pd.DataFrame(ch_list)   

    for index,row in df.iterrows():    
        insert_query= '''Insert Into channels(Channel_Name,
                                                 Channel_Id,
                                                 Subscribers,
                                                 Views,
                                                 Total_Videos,
                                                 Channel_Description, 
                                                 Playlist_Id)

                                                 VALUES(%s,%s,%s,%s,%s,%s,%s)'''

        values=(row['Channel_Name'],
                row['Channel_Id'],
                row['Subscribers'],
                row['Views'], 
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_Id'])
        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print(" Channels Values are alredy inserted")
         
        
def playlist_table():
    mydb=psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sqldir",
                          database="youtube_data",
                          port="5432")

    cursor=mydb.cursor()

    drop_query = "Drop Table if exists playlists"
    cursor.execute(drop_query)
    mydb.commit()

    try:
        create_query='''Create Table If not exists playlists(Playlist_Id varchar(100) primary key,
                                                            Title varchar(100),
                                                            Channel_Id varchar(100),
                                                            Channel_Name varchar(100),
                                                            Published_At timestamp,
                                                            Video_Count int)'''

        cursor.execute(create_query)
        mydb.commit()

    except:
        print("Playlists Table Already Created")


    pl_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for pl_data in collection1.find({},{"_id":0,"Playlist_Information":1}):
        for i in range(len(pl_data["Playlist_Information"])):
            pl_list.append(pl_data["Playlist_Information"][i])
    df1=pd.DataFrame(pl_list)


    for index,row in df1.iterrows():
        insert_query = '''INSERT INTO playlists(Playlist_Id,
                                                    Title,
                                                    Channel_Id,
                                                    Channel_Name,
                                                    Published_At,
                                                    Video_Count)

                                                    VALUES(%s,%s,%s,%s,%s,%s)'''

        values=(
                row['Playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'], 
                row['Published_At'],
                row['Video_Count'])


        try:
            cursor.execute(insert_query,values)
            mydb.commit()

        except:
            print("playlists Values are alredy inserted")

            
def videos_table():
    mydb=psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sqldir",
                          database="youtube_data",
                          port="5432")

    cursor=mydb.cursor()

    drop_query='''Drop Table if exists Videos'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''Create Table If not exists Videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(100),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration interval,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                    )'''
    cursor.execute(create_query)
    mydb.commit()


    vi_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for vi_data in collection1.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(vi_data["Video_Information"])):
            vi_list.append(vi_data["Video_Information"][i])
    df2=pd.DataFrame(vi_list)


    for index,row in df2.iterrows():
        insert_query ='''
                      INSERT INTO videos(Channel_Name,
                                    Channel_Id,
                                    Video_Id,
                                    Title,
                                    Tags,
                                    Thumbnail,
                                    Description,
                                    Published_Date,
                                    Duration,
                                    Views,
                                    Likes,
                                    Comments,
                                    Favorite_Count,
                                    Definition,
                                    Caption_Status)

                                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                    '''

        values = (row['Channel_Name'],
                    row['Channel_Id'],
                    row['Video_Id'],
                    row['Title'],
                    row['Tags'],
                    row['Thumbnail'],
                    row['Description'],
                    row['Published_Date'],
                    row['Duration'],
                    row['Views'],
                    row['Likes'],
                    row['Comments'],
                    row['Favorite_Count'],
                    row['Definition'],
                    row['Caption_Status'])
        try:       
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Video values already inserted")


def comments_table():
    mydb=psycopg2.connect(host="localhost",
                          user="postgres",
                          password="sqldir",
                          database="youtube_data",
                          port="5432")

    cursor=mydb.cursor()

    drop_query='''Drop Table if exists comments'''
    cursor.execute(drop_query)
    mydb.commit()

    create_query='''Create Table If not exists comments(Comment_Id varchar(100) primary key,
                                                      Video_Id varchar(100),
                                                      Comment_Text text,
                                                      Comment_Author varchar(100),
                                                      Comment_Published timestamp)'''

    cursor.execute(create_query)
    mydb.commit()


    com_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for com_data in collection1.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(com_data["Comment_Information"])):
            com_list.append(com_data["Comment_Information"][i])
    df3=pd.DataFrame(com_list)


    for index,row in df3.iterrows():
        insert_query ='''
                      INSERT INTO comments(Comment_Id,
                                          Video_Id,
                                          Comment_Text ,
                                          Comment_Author,
                                          Comment_Published)

                                    VALUES(%s, %s, %s, %s, %s) '''


        values = (row['Comment_Id'],
                    row['Video_Id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_Published'])
        try:       
            cursor.execute(insert_query,values)
            mydb.commit()
        except:
            print("Commets Values already inserted")

            
def tables():
    channels_table()
    playlist_table()
    videos_table()
    comments_table()
    
    return "Tables Created successfully"


def show_channels_table():
    ch_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({},{"_id":0,"Channel_Information":1}):
        ch_list.append(ch_data["Channel_Information"])
    df=st.dataframe(ch_list) 
    
    return df


def show_playlists_table():
    pl_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for pl_data in collection1.find({},{"_id":0,"Playlist_Information":1}):
        for i in range(len(pl_data["Playlist_Information"])):
            pl_list.append(pl_data["Playlist_Information"][i])
    df1=st.dataframe(pl_list)
    
    return df1


def show_videos_table():
    vi_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for vi_data in collection1.find({},{"_id":0,"Video_Information":1}):
        for i in range(len(vi_data["Video_Information"])):
            vi_list.append(vi_data["Video_Information"][i])
    df2=st.dataframe(vi_list)
    
    return df2


def show_comments_table():
    com_list=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for com_data in collection1.find({},{"_id":0,"Comment_Information":1}):
        for i in range(len(com_data["Comment_Information"])):
            com_list.append(com_data["Comment_Information"][i])
    df3=st.dataframe(com_list)
    
    return df3


#Streamlit :

with st.sidebar:
    st.title(":silver[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill TakeAway")
    st.caption("Python Scripting")
    st.caption("Data Collection")    
    st.caption("Mongo DB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")
    
channel_id=st.text_input("Enter the Channel ID") 

if st.button("Collect and Store Data"):
    ch_ids=[]
    db=client["youtube_data"]
    collection1=db["Channel_details"]
    for ch_data in collection1.find({},{"_id":0,"Channel_Information":1}):
        ch_ids.append(ch_data["Channel_Information"]["Channel_Id"])
                                              
    if channel_id in ch_ids:
        st.success("Channel Details of given ID already exist")
    else:
        insert=Channel_details(channel_id)
        st.success(insert)
        
if st.button("Migrate to SQL"):
    Table=tables()
    st.success(Table)
    
show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))    
   
if show_table=="CHANNELS":
    show_channels_table()
    
elif show_table=="PLAYLISTS":
    show_playlists_table()
    
elif show_table=="VIDEOS":
    show_videos_table()
    
elif show_table=="COMMENTS":
    show_comments_table()

    
#SQL Connection:
mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="sqldir",
                      database="youtube_data",
                      port="5432")

cursor=mydb.cursor()

Question=st.selectbox("Select your Question",('1. All the videos and the Channel Name',
                                              '2. Channels with most number of videos',
                                              '3. 10 most viewed videos',
                                              '4. Comments in each video',
                                              '5. Videos with highest likes',
                                              '6. likes of all videos',
                                              '7. views of each channel',
                                              '8. videos published in the year 2022',
                                              '9. average duration of all videos in each channel',
                                             '10. videos with highest number of comments'))

#QUERY 1:
mydb=psycopg2.connect(host="localhost",
                      user="postgres",
                      password="sqldir",
                      database="youtube_data",
                      port="5432")

cursor=mydb.cursor()

if Question=="1. All the videos and the Channel Name":
    query1='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["Video Title","Channel Name"])
    st.write(df)
    
#QUERY 2:
elif Question=='2. Channels with most number of videos':
    query2='''select channel_name as channelname,total_videos as no_videos from channels
               order by total_videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2=cursor.fetchall()
    df2=pd.DataFrame(t2,columns=["Channel Name","No of Videos"])
    st.write(df2)  
    
#QUERY 3:
elif Question=='3. 10 most viewed videos':
    query3='''select views as views,channel_name as channelname,title as videotitle from videos
              where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3=cursor.fetchall()
    df3=pd.DataFrame(t3,columns=["Viwes","Channel Name","Videotitle"])
    st.write(df3)
    
#QUERY 4:
elif Question=='4. Comments in each video':
    query4='''select comments as no_comments,title as videotitle from videos where
               comments is not null'''
    cursor.execute(query4)
    mydb.commit()
    t4=cursor.fetchall()
    df4=pd.DataFrame(t4,columns=["No of comments","Videotitle"])
    st.write(df4)
    
#QUERY 5:
elif Question=='5. Videos with highest likes':
    query5='''select title as videotitle,channel_name as channelname,Likes as likecount
              from videos where Likes is not null order by Likes desc'''
    cursor.execute(query5)
    mydb.commit()
    t5=cursor.fetchall()
    df5=pd.DataFrame(t5,columns=["Videotitle","Channle Name","likecount"])
    st.write(df5)
    
#QUERY 6:   
elif Question=='6. likes of all videos':
    query6='''select Likes as likecount,title as videotitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6=cursor.fetchall()
    df6=pd.DataFrame(t6,columns=["Likecount","Videotitle"])
    st.write(df6)
    
#QUERY 7:  
elif Question=='7. views of each channel':
    query7='''select channel_name as channelname,views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7=cursor.fetchall()
    df7=pd.DataFrame(t7,columns=["Channel Name","Total Views"])
    st.write(df7)
    
#QUERY 8:
elif Question=='8. videos published in the year 2022':
    query8='''select title as video_title,Published_Date as Videorelease,channel_name as channelname from videos
              where extract(year from Published_Date)=2022'''
    cursor.execute(query8)
    mydb.commit()
    t8=cursor.fetchall()
    df8=pd.DataFrame(t8,columns=["Video Title","Published Date","Channel Name"])
    st.write(df8)
    
#QUERY 9:  
elif Question=='9. average duration of all videos in each channel':
    query9='''select channel_name as channelname,AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9=cursor.fetchall()
    df9=pd.DataFrame(t9,columns=["ChannelName","AverageDuration"])
    
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["ChannelName"]
        average_duration=row["AverageDuration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,averageduration=average_duration_str))
    df1=pd.DataFrame(T9)
    st.write(df1)
    
# QUERY 10:
elif Question=='10. videos with highest number of comments':
    query10='''select title as videotitle,channel_name as channelname,Comments as comments from videos
                where Comments is not null order by Comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10=cursor.fetchall()
    df10=pd.DataFrame(t10,columns=["Video Title","Channel Name","Comments"])
    st.write(df10)
            
            

            
            



