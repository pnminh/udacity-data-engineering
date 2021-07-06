from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries, DataValidationTests

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly',
          catchup=False
          )

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = DummyOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="udacity_creds",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    s3_region="us-west-2",
    jsonpaths_file='s3://udacity-dend/log_json_path.json'
)

stage_songs_to_redshift = DummyOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="udacity_creds",
    s3_bucket="minh-udacity-dend",
    s3_key="song_data",
    s3_region="us-east-1"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    fact_table_name="songplays",
    fact_table_insert_query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table_name="users",
    dimension_table_insert_query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table_name="songs",
    dimension_table_insert_query=SqlQueries.song_table_insert
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table_name="artists",
    dimension_table_insert_query=SqlQueries.artist_table_insert
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    dimension_table_name="time",
    dimension_table_insert_query=SqlQueries.time_table_insert
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    test_function=DataValidationTests().test_data_exist,
    test_function_args=(
        "redshift", ("staging_events", "staging_songs", "songplays", "artists", "users", "songs", "time"))

)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator
