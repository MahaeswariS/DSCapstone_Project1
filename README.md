# YouTube-Data-Harvesting-and-Warehousing-using-PostgreSQL-MongoDB-and-Streamlit
#### Problem Statement
The task is to build a Streamlit app that permits users to access and analyze data from multiple YouTube channels.
Users should be able to input a YouTube channel ID and retrieve channel information, video details, user engagement data namely likes and comments on each video. The app should facilitate collecting data for upto 10 different YouTube channels and storing them in a MongoDB database by clicking a button.It should offer the capability to migrate the selected channel data from the data lake(MongoDB) to a SQL database(PostgreSQL) for further analysis. The app should enable searching and retrieval of data from the SQL database, including advanced options like joining tables for comprehensive channel information.

#### Technology Stack Used
1.Google Client Library
The googleapiclient library in Python facilitates the communication with different Google APIs. Its primary purpose in this project is to interact with YouTube's Data API v3, allowing the retrieval of essential information like channel details, playlist, video specifics, and comments. By utilizing googleapiclient, developers can easily access and manipulate YouTube's extensive data resources through code.

2.Python
Python is a powerful programming language renowned for being easy to learn and understand. Python is the primary language used in this project for the development of the complete application, including data retrieval, processing, analysis, and visualisation.

3.MongoDB
MongoDB is built on a scale-out architecture that has become popular with developers of all kinds for developing scalable applications with evolving data schemas. As a document database, MongoDB makes it easy for developers to store structured or unstructured data. It uses a JSON-like format to store documents.

4.PostgreSQL
PostgreSQL is an open-source, advanced, and highly scalable database management system (DBMS) known for its reliability and extensive features. It provides a platform for storing and managing structured data, offering support for various data types and advanced SQL capabilities.

5.Streamlit
Streamlit library was used to create a user-friendly UI that enables users to interact with the program and carry out data retrieval and data analysis

REQUIRED LIBRARIES:

1.googleapiclient.discovery

2.streamlit

3.pymongo

4.psycopg2

5.pandas

#### Approach
Start by setting up a Streamlit application using the python library "streamlit", which provides an easy-to-use interface for users to enter a YouTube channel ID, view channel details, and select channels to migrate.
Establish a connection to the YouTube API V3, which allows user to request and retrieve channel and video data by utilizing the Google API client library for Python.
Store the retrieved data in a MongoDB data lake, as it is a suitable choice for handling unstructured and semi-structured data. This is done by firstly writing a method to retrieve the previously called api call and storing the extracted data in the MongoDB collections.
Migrate the collected data from multiple channels namely the channels, playlists, videos and comments to a SQL data warehouse, utilizing a PostgreSQL database.
Execute SQL queries to join tables within the SQL data warehouse and retrieve specific channel data based on user input.
The retrieved data is displayed within the Streamlit application, leveraging Streamlit's data visualization capabilities to create charts and graphs for users to analyze the data by utilizing plotly library.