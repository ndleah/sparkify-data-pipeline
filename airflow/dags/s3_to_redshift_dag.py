# Import necessary modules and libraries
from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

# Import custom operators and helpers
from operators import StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator
from helpers import SqlQueries

# Define default arguments for the DAG
default_args = {
    'owner': 'ndleah',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': 300,
    'email_on_retry': False
}

# Create the DAG instance with relevant information
dag = DAG('s3_to_redshift_dag',
        default_args=default_args,
        description='Extract Load and Transform data from S3 to Redshift',
        schedule_interval='@hourly',
        catchup=False
        )

# Define the starting operator for the DAG
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# Create PostgreSQL table creation operators for staging and dimension tables
create_staging_events_table = PostgresOperator(
    task_id='Create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=SqlQueries.staging_events_table_create
)

# ... (similar create_staging_songs_table, create_songplays_table, etc.)

# Define a dummy operator to signify successful schema creation
schema_created = DummyOperator(task_id='Schema_created', dag=dag)

# Define operators to stage data from S3 to Redshift
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    s3_bucket='sparkify-pipeline',
    s3_prefix='log_data',
    table='staging_events',
    copy_options="JSON 's3://sparkify-pipeline/log_json_path.json'"
)

# ... (similar stage_songs_to_redshift)

# Define operators to load data into fact and dimension tables
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    select_sql=SqlQueries.songplays_table_insert
)

# ... (similar load_user_dimension_table, load_song_dimension_table, etc.)

# Define data quality checks operator
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    check_stmts=[
        {
            'sql': 'SELECT COUNT(*) FROM songplays;',
            'op': 'gt',
            'val': 0
        },
        {
            'sql': 'SELECT COUNT(*) FROM songplays WHERE songid IS NULL;',
            'op': 'eq',
            'val': 0
        }
    ]
)

# Define the end operator for the DAG
end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# Set up dependencies between operators
# ... (operator dependencies)

# DAG dependencies
start_operator >> create_staging_songs_table
start_operator >> create_staging_events_table
# ... (similar dependencies)

create_staging_events_table >> schema_created
create_staging_songs_table >> schema_created
# ... (similar dependencies)

schema_created >> stage_events_to_redshift
schema_created >> stage_songs_to_redshift
# ... (similar dependencies)

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
# ... (similar dependencies)

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table
# ... (similar dependencies)

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
