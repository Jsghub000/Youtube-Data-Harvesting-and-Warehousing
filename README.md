# Youtube-Data-Harvesting-and-Warehousing
Capstone Project:  Youtube Data Harvesting and Warehousing
This project aims to give users the opportunity to access and evaluate data from several YouTube channels .SQL, MongoDB, and Streamlit are used in the project to provide a user-friendly application for retrieving, saving, and querying YouTube channel and video data.

Libraries to import:

1. Googleapiclient.Discovery.

2. Streamlit.

   3.psycopg2.

4. PyMongo.

5. Pandas.

Approach:

Streamlit application: A simple Streamlit application has been developed where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse and get required details from the tables created through queries.

Connect to the YouTube API: The Google API client library has to be created for Python to make requests to the API.

Store data in a MongoDB database: Once the data has been retrieved from the YouTube API, it is then stored in a MongoDB database.

Migrate data to a SQL data warehouse: PostgreSQL has been used to migrate the MongoDB data to a Structured table.

Query the SQL data warehouse: SQL queries are used to retrieve the data for the given particular scenario.

Workflow:

The user has to give a valid channel ID in the given text box. In the case of an invalid channel ID, a warning message will be displayed. Also for already added channel details, a message will be displayed accordingly.

With the click of the button "collect and store", the data is fetched from YouTube using the API key and it is stored in the MongoDB repository. Either if the channel ID given is already stored, or for an invalid channel ID a proper message will be popped.

With the click of the button "Generate", the channel table, playlist table, videos table, and comment table will be created.

The entire table can be viewed using the tab created for the purpose.

The recommended 10 queries have been created as a drop-down list, the user can select any query to view the result.

