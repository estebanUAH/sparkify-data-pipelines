from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries



default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

# Create DAG
@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False,
    max_active_runs=1
)
def final_project():
    """
    This DAG orchestrates the ETL process for the Sparkify music streaming company.
    It stages data from S3 to Redshift, loads fact and dimension tables, and
    performs data quality checks.
    """

    # Begin execution
    start_operator = EmptyOperator(task_id='Begin_execution')

    # Stage events
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_bucket="eligero-data-pipelines",
        s3_key="log-data",
        json_path="s3://eligero-data-pipelines/log_json_path.json",
        region="us-east-1"
    )

    # Stage songs
    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="eligero-data-pipelines",
        s3_key="song-data",
        json_path="auto",
        region="us-east-1"
    )

    # Load songplays fact table
    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id="redshift",
        table="songplays",
        sql_query=SqlQueries.songplay_table_insert
    )

    # Load user dimension table
    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id="redshift",
        table="users",
        sql_query=SqlQueries.user_table_insert,
        truncate_table=True
    )

    # Load song dimension table
    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id="redshift",
        table="songs",
        sql_query=SqlQueries.song_table_insert,
        truncate_table=True
    )

    # Load artist dimension table
    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id="redshift",
        table="artists",
        sql_query=SqlQueries.artist_table_insert,
        truncate_table=True
    )

    # Load time dimension table
    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id="redshift",
        table="time",
        sql_query=SqlQueries.time_table_insert,
        truncate_table=True
    )

    # Run data quality checks
    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        tables=["songplays", "users", "songs", "artists", "time"]
    )

    # Stop execution
    stop_execution = EmptyOperator(task_id='Stop_execution')

    # Define task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks
    run_quality_checks >> stop_execution

final_project_dag = final_project()
