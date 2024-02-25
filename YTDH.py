import pprint
import googleapiclient.discovery
import googleapiclient.errors
import json
import pymongo
from pymongo import MongoClient
import mysql.connector
import json
import mysql.connector
import streamlit as st

chID = st.text_input("Enter Channel Id")
#chID="UCduIoIMfD8tT3KoU0-zBRgQ"
apiKey="AIzaSyCavZTVfOycz7mRQG_8Px-_cd6h7GHpf-k"

api_service_name = "youtube"
api_version = "v3"

uri = "mongodb://localhost:27017/"

client = pymongo.MongoClient(uri)
db = client["youtubedb"]
channels_collection = db["channels"]

#channel_video_comment_dict = {}
#v1 = None
#@st.cache(suppress_st_warning=True)



def extract_channel_data(Channel_ID):

    try:

        global channel_video_comment_dict

        channel_video_comment_dict ={}

        youtube = googleapiclient.discovery.build(
        api_service_name, api_version, developerKey=apiKey)

        request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=chID
        )
        response = request.execute()

        #pprint.pprint(response)
        #st.write(response)
        channel_nm = response["items"][0]["snippet"]["title"]



        # Check if the channel name exists in the channel collection
        Get_Channel_Name = channels_collection.find_one({'Channel_Name.Channel_Name': channel_nm})

        if Get_Channel_Name is None:

            Playlist_ID = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

            def fetch_all_video_ids(inp_playlist_id):
                chl_all_video_ids = []

                next_page_token = None

                while True:
                    pl_request = youtube.playlistItems().list(
                        part="contentDetails",
                        maxResults=25,
                        playlistId=inp_playlist_id,
                        pageToken=next_page_token
                    )
                    pl_response = pl_request.execute()

                    # Extract the video IDs from the response and add them to the chl_all_videos list
                    for i in pl_response["items"]:
                        video_id = i["contentDetails"]["videoId"]
                        chl_all_video_ids.append(video_id)

                    # Check in the given channel if there are more pages to read
                    # If no more pages present then exit the loop
                    next_page_token = pl_response.get("nextPageToken")
                    if not next_page_token:
                        break  

                return chl_all_video_ids

            ch_all_video_ids = fetch_all_video_ids(Playlist_ID)

            #print(ch_all_video_ids)
            
            channel_data = {
            "Channel_Name":{
            "Channel_Name": response["items"][0]["snippet"]["title"],
            "Channel_Id": response["items"][0]["id"],
            "Subscription_Count": response["items"][0]["statistics"]["subscriberCount"],
            "Channel_Views": response["items"][0]["statistics"]["viewCount"],
            "Channel_Description": response["items"][0]["snippet"]["description"],
            "Playlist_Id": response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"],
            } 
            }

            channel_video_comment_dict.update(channel_data)

            cnt1 = 0
            for i in ch_all_video_ids:
                cnt1 += 1
                vd_request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    id=i
                )
                vd_response = vd_request.execute()
                
                #print(i)

                video_data = {"Video_Id_"+str(cnt1):{
                "Video_Id": vd_response["items"][0]["id"],
                "Video_Name": vd_response["items"][0]["snippet"]["title"],
                "Video_Description": vd_response["items"][0]["snippet"]["description"],
                "Tags": vd_response["items"][0]["snippet"].get("tags", []),
                "PublishedAt": vd_response["items"][0]["snippet"]["publishedAt"],
                "View_Count": vd_response["items"][0]["statistics"]["viewCount"],
                "Like_Count": vd_response["items"][0]["statistics"]["likeCount"],
                "Dislike_Count": "0",
                "Favorite_Count": vd_response["items"][0]["statistics"]["favoriteCount"],
                "Comment_Count": vd_response["items"][0]["statistics"].get("commentCount", 0),
                "Duration": vd_response["items"][0]["contentDetails"]["duration"],
                "Thumbnail": vd_response["items"][0]["snippet"]["thumbnails"]["default"]["url"],
                "Caption_Status": "Available" if vd_response["items"][0]["contentDetails"]["caption"] else "Not Available"
                }
                }

                video_data[f"Video_Id_{cnt1}"]["Comments"] = {}

                channel_video_comment_dict.update(video_data)

                if int(vd_response["items"][0]["statistics"].get("commentCount", 0)):
                    cm_request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=i
                    )
                    #print(i)
                    cm_response = cm_request.execute()
                    #print(cm_response)
                    cnt2 = 0
                    for comment_item in cm_response["items"]:
                        cnt2 += 1
                        comment_data = {
                            "Comment_Id_" + str(cnt2): {
                                "Comment_Id": comment_item["snippet"]["topLevelComment"]["id"],
                                "Comment_Text": comment_item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                                "Comment_Author": comment_item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                                "Comment_PublishedAt": comment_item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                            }
                        }
                        
                        channel_video_comment_dict[f"Video_Id_{cnt1}"]["Comments"].update(comment_data)
                #else:
                    #print(f"Comments are disabled for the video with ID {i}. Skipping comment retrieval.")
                    
            #print(channel_video_comment_dict)

            #return channel_video_comment_dict
            
            client.close()
            #st.write(channel_video_comment_dict)
            return channel_video_comment_dict, 1
        else:
            client.close()
            return 0,0
    except Exception as err:
        client.close()
        st.write("An unexpected error occured: ", err)
        traceback.print_exc()
        return None
    except googleapiclient.errors.HttpError as http_err:
        client.close()
        error_message = json.loads(http_err.content)['error']['message']
        st.write(f"An HTTP error occurred: {error_message}")
        

#channel_video_comment_dictionary = extract_channel_data(chID)

#def load_youtube_data_to_mongodb(v2):
#    channels_collection.insert_one(v2) #channel_video_comment_dict)
#    st.success("Channel data is inserted successfully into MongoDB")




distinct_values = channels_collection.distinct("Channel_Name.Channel_Name")



def migrate_channel_data_from_mongo_to_mysql(selected_chl):

    try:

        uri1 = "mongodb://localhost:27017/"

        client1 = pymongo.MongoClient(uri1)
        db1 = client["youtubedb"]
        channels_collection1 = db["channels"]

        mysql_config = {
        'user':'dev_user', 'password':'My$ql123',
        'host':'127.0.0.1', 'database':'youtubedb'
        }

        mysql_connect = mysql.connector.connect(**mysql_config)

        mysql_cursor = mysql_connect.cursor()
        
        channel_documents = channels_collection1.find_one({"Channel_Name.Channel_Name": selected_chl})

        #st.write(channel_documents["Channel_Name"])
        # Retrieve data from MongoDB collection
        #channel_documents = 1 #channels_collection1({})

        delete_sql_queries = [
            "DELETE FROM youtubedb.comment;",
            "DELETE FROM youtubedb.video;",
            "DELETE FROM youtubedb.playlist;",
            "DELETE FROM youtubedb.channel;"
        ]

        for query in delete_sql_queries:
            mysql_cursor.execute(query)

        #st.write(type(channel_documents))  
        # Extract data from MongoDB document
        #for channel_document in channel_documents:
        #   st.write("1111")
        #   st.write("222",channel_document,channel_documents["Channel_Name"]["Channel_Name"])
        #channel_data = channel_document["Channel_Name"]["Channel_Name"]
        channel_id = channel_documents["Channel_Name"]["Channel_Id"]
        channel_name = channel_documents["Channel_Name"]["Channel_Name"]
        subscription_count = channel_documents["Channel_Name"]["Subscription_Count"]
        channel_views = channel_documents["Channel_Name"]["Channel_Views"]
        channel_description = channel_documents["Channel_Name"]["Channel_Description"]
        playlist_id = channel_documents["Channel_Name"]["Playlist_Id"]

            #mysql_cursor.execute(
            #"delete from youtubedb.video; delete from youtubedb.playlist; delete from youtubedb.channel;"
            #)

        mysql_cursor.execute(
        "INSERT INTO channel (channel_id, channel_name, subscription_count, channel_views, channel_description) VALUES (%s, %s, %s, %s, %s)",
        (channel_id, channel_name, subscription_count, channel_views, channel_description)
        )

        mysql_cursor.execute(
        "INSERT INTO playlist (playlist_id, channel_id) VALUES (%s, %s)",
        (playlist_id, channel_id)
        )

        for key, value in channel_documents.items():
            if key.startswith("Video_Id"):
                video_data = value
                #print(video_data)
                video_id = video_data.get("Video_Id", "")
                video_name = video_data.get("Video_Name", "")
                video_description = video_data.get("Video_Description", "")
                published_date = video_data.get("PublishedAt", "").replace("T", " ").replace("Z", "")
                view_count = video_data.get("View_Count", "")
                like_count = video_data.get("Like_Count", "")
                dislike_count = video_data.get("Dislike_Count", "")
                favorite_count = video_data.get("Favorite_Count", "")
                comment_count = video_data.get("Comment_Count", "")
                duration = eval(video_data.get("Duration", "").replace("P0D","0").replace("PT","").replace("S","").replace("H","*3600+").replace("M","*60+") 
                            if "S" in video_data.get("Duration", "")
                            else (video_data.get("Duration", "").replace("P0D","0").replace("PT","").replace("S","").replace("H","*3600+").replace("M","*60")
                                    if "M" in video_data.get("Duration", "") 
                                    else video_data.get("Duration", "").replace("P0D","0").replace("PT","").replace("H","*3600")) 
                            )
                thumbnail = video_data.get("Thumbnail", "")
                caption_status = video_data.get("Caption_Status", "")
                
                mysql_cursor.execute(
                    "INSERT IGNORE INTO video (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (video_id, playlist_id, video_name, video_description, published_date, view_count, like_count, dislike_count, favorite_count, comment_count, duration, thumbnail, caption_status)
                )

                for key, value in video_data["Comments"].items():
                    if isinstance(value, dict):
                        comment_data = value
                        comment_id = comment_data.get("Comment_Id", "")
                        comment_text = comment_data.get("Comment_Text", "")
                        comment_author = comment_data.get("Comment_Author", "")
                        comment_published_date = comment_data.get("Comment_PublishedAt", "")

                        mysql_cursor.execute(
                            "INSERT IGNORE INTO comment (comment_id, video_id, comment_text, comment_author, comment_published_date) VALUES (%s, %s, %s, %s, %s)",
                            (comment_id, video_id, comment_text, comment_author, comment_published_date)
                        )


        mysql_connect.commit()

        # Closing connections
        client1.close()
        mysql_cursor.close()
        mysql_connect.close()

        st.write("Selected Channel Data migrated to Mysql")
    except Exception as err:
        client.close()
        st.write("An unexpected error occured: ", err)
        traceback.print_exc()
        return None 

def youtube_stats(selected_qry):
    st.write(selected_qry,type(selected_qry))

def page1():
    #st.sidebar("st_test.py", label="Home", icon="🏠")
    #st.sidebar.page_link("st_test.py", label="ETL", icon="🔧🔩🔨")
    #st.sidebar.page_link("st_test.py", label="View", icon="👁️‍🗨️")
    #"How would you like to be contacted?",
    #("Email", "Home phone", "Mobile phone"))

    st.title("YOUTUBE HARVEST")

    if st.button("Extract Data"):
        value1,rerutn_code1 = extract_channel_data(chID)
        if rerutn_code1 == 0:
            st.error("Channel data aleady available, please enter different Channel Id")
        else:
            st.success("Channel data extracted successfully")
        
    if st.button("Upload to MongoDB"):
        value2,return_code2 = extract_channel_data(chID)
        if return_code2 == 0:
            st.error("Channel data aleady available, please enter different Channel Id")
        else:
            channels_collection.insert_one(value2)
            st.success("Channel data is inserted successfully into MongoDB")

    selected_channel = st.selectbox("Select a CHannel to begin Trasformation to SQL",distinct_values)

    if st.button("Submit"):
        migrate_channel_data_from_mongo_to_mysql(selected_channel)

    st.title("Select any questions to get Insights")

    selected_query = st.selectbox("Questions",
                                    ("1. What are the names of all the videos and their corresponding channels?",
                                    "2. Which channels have the most number of videos, and how many videos do they have?",
                                    "3. What are the top 10 most viewed videos and their respective channels?",
                                    "4. How many comments were made on each video, and what are their corresponding video names?",
                                    "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
                                    "6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
                                    "7. What is the total number of views for each channel, and what are their corresponding channel names?",
                                    "8. What are the names of all the channels that have published videos in the year 2022?",
                                    "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"
                                    )
                                )
    
    if selected_query:
        youtube_stats(int(selected_query.split(r".")[0]))


page1()
