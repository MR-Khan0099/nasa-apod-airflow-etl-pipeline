# from airflow import DAG
# from airflow.providers.http.operators.http import SimpleHttpOperator
# from airflow.decorators import task
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.utils.dates import days_ago
# import json

# # Define the DAG
# with DAG(
#     dag_id ='nasa_apod_postgres_dag',
#     start_date=days_ago(1),
#     schedule='@daily',
#     catchup=False,
# ) as dag:
    
#     # step 1: Create the table if it doesn't exist
#     @task
#     def create_table():
#         ## Initialize the PostgresHook --> used to interact with PostgreSQL
#         pg_hook = PostgresHook(postgres_conn_id='postgres_connection')

#         # sql query to create the table
#         create_table_sql = """
#         CREATE TABLE IF NOT EXISTS nasa_apod_data (
#             id SERIAL PRIMARY KEY,
#             title VARCHAR(255),
#             explanation TEXT,
#             url TEXT,
#             date DATE,
#             title TEXT,
#             media_type VARCHAR(50),
#         );
#         """
#         # Execute the SQL query to create the table
#         pg_hook.run(create_table_sql)


#     # Step 2: Extract data from the NASA API [Extract pipeline  ]
#     # https://api.nasa.gov/planetary/apod?api_key=IHvuVaPfjZRyfaSpmVa4b1bwpQSRZEkmgzR3XxUy
#     extract_apod = SimpleHttpOperator(
#         task_id='extract_apod_data',
#         http_conn_id='nasa_api', # Connection ID defined in airflow for the NASA API
#         endpoint='planetary/apod', #nasa api endpoint for apod
#         method='GET',
#         data ={"api_key":"{{conn.nasa_api.extra_dejson.api_key}}"}, # use the apikey from the connection 
#         response_filter=lambda response: response.json(), # filter the response to get the json data
#     )

    

#     # Step 3: Transform the data (Pick the information that i need to save ) [Transform pipeline]
#     @task
#     def transform_apod_data(response):
#         # Extract the relevant fields from the response
#         transformed_data = {
#             'title': response.get('title', ''),
#             'explanation': response.get('explanation', ''),
#             'url': response.get('url', ''),
#             'date': response.get('date', ''),
#             'media_type': response.get('media_type', '')
#         }
#         return transformed_data
    

#     # step 4: Load the data into PostgreSQL [Load pipeline]
#     @task
#     def load_data_to_postgres(transformed_data):
#         # Initialize the PostgresHook
#         pg_hook = PostgresHook(postgres_conn_id='postgres_connection')

#         # SQL query to insert data into the table
#         insert_sql = """
#         INSERT INTO nasa_apod_data ( title, explanation, url, date, media_type)
#         VALUES (%s, %s, %s, %s, %s)

#         """
#         # Execute the SQL query to insert data
#         pg_hook.run(insert_sql, paramters=(
#             transformed_data['title'],
#             transformed_data['explanation'],
#             transformed_data['url'],
#             transformed_data['date'],
#             transformed_data['media_type']
#         ))



#     # step 5: Verify the data DBViewer

#     # step 6: Define the task dependencies
#     #extract
#     create_table() >> extract_apod  # Ensure the table is created before extracting data 
#     api_response = extract_apod.output  # Get the output of the extract task 
#     # Transform the extracted data
#     transform_data = transform_apod_data(api_response)  # Transform the extracted data
#     # Load the transformed data into PostgreSQL
#     load_data_to_postgres(transform_data)  # Load the transformed data into PostgreSQL
   
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import json


## Define the DAG
with DAG(
    dag_id='nasa_apod_postgres',
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False

) as dag:
    
    ## step 1: Create the table if it doesnt exists

    @task
    def create_table():
        ## initialize the Postgreshook
        postgres_hook=PostgresHook(postgres_conn_id="my_postgres_connection")

        ## SQL query to create the table
        create_table_query="""
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );


        """
        ## Execute the table creation query
        postgres_hook.run(create_table_query)


    ## Step 2: Extract the NASA API Data(APOD)-Astronomy Picture of the Day[Extract pipeline]
    ## https://api.nasa.gov/planetary/apod?api_key=7BbRvxo8uuzas9U3ho1RwHQQCkZIZtJojRIr293q
    extract_apod=SimpleHttpOperator(
        task_id='extract_apod',
        http_conn_id='nasa_api',  ## Connection ID Defined In Airflow For NASA API
        endpoint='planetary/apod', ## NASA API enpoint for APOD
        method='GET',
        data={"api_key":"{{ conn.nasa_api.extra_dejson.api_key}}"}, ## USe the API Key from the connection
        response_filter=lambda response:response.json(), ## Convert response to json
    )

    

    ## Step 3: Transform the data(Pick the information that i need to save)
    @task
    def transform_apod_data(response):
        apod_data={
            'title': response.get('title', ''),
            'explanation': response.get('explanation', ''),
            'url': response.get('url', ''),
            'date': response.get('date', ''),
            'media_type': response.get('media_type', '')

        }
        return apod_data


    ## step 4:  Load the data into Postgres SQL
    @task
    def load_data_to_postgres(apod_data):
        ## Initialize the PostgresHook
        postgres_hook=PostgresHook(postgres_conn_id='my_postgres_connection')

        ## Define the SQL Insert Query

        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        ## Execute the SQL Query

        postgres_hook.run(insert_query,parameters=(
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']


        ))



    ## step 5: Verify the data DBeaver


    ## step 6: Define the task dependencies
    ## Extract
    create_table() >> extract_apod  ## Ensure the table is create befor extraction
    api_response=extract_apod.output
    ## Transform
    transformed_data=transform_apod_data(api_response)
    ## Load
    load_data_to_postgres(transformed_data)