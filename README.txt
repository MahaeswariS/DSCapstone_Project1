# YouTube-Data-Harvesting-and-Warehousing-using-PostgreSQL-MongoDB-and-Streamlit
#### Problem Statement
The task is to build a Streamlit app that permits users to access and analyze data from multiple YouTube channels.
Users should be able to input a YouTube channel ID and retrieve channel information, video details, user engagement data namely likes and comments on each video. The app should facilitate collecting data for upto 10 different YouTube channels and storing them in a MongoDB database by clicking a button.It should offer the capability to migrate the selected channel data from the data lake(MongoDB) to a SQL database(PostgreSQL) for further analysis. The app should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.

#### Technology Stack Used
Google Client Library
Python
MongoDB
PostgreSQL

#### Approach
Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.
Establish a connection to the YouTube API V3, which allows me to retrieve channel and video data by utilizing the Google API client library for Python.
Store the retrieved data in a MongoDB data lake, as MongoDB is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the extracted data in the MongoDB collections.
Transferring the collected data from multiple channels namely the channels, playlists, videos and comments to a SQL data warehouse, utilizing a PostgreSQL database.
Utilize SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input. For that the SQL table previously made has to be properly given the the foreign and the primary key.
The retrieved data is displayed within the Streamlit application, leveraging Streamlit's data visualization capabilities to create charts and graphs for users to analyze the data.