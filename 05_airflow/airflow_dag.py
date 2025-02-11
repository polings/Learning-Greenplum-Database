from airflow import DAG
from datetime import datetime, timedelta, date
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

DB_CONN = "gp_std9_121"
DB_SCHEMA = 'std9_121'
DB_PROC_LOAD = ['f_full_load', 'f_load_delta_partitions']
MART_PROC = 'f_load_mart'
PARTITION_LOAD_TABLES = ['sales', 'plan']
FULL_LOAD_TABLES = ['region', 'price', 'chanel', 'product']
FULL_LOAD_FILES = {'region': 'region', 'price': 'price', 'chanel': 'chanel', 'product': 'product'}
START_DATE, END_DATE = '2021-01-01', '2021-08-01'

MD_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD[0]}(%(ext_tab_name)s, %(tab_name)s, %(truncate_tgt)s);"
FACT_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{DB_PROC_LOAD[1]}(%(ext_tab_name)s, %(tab_name)s, '\"date\"', %(start_date)s::timestamp, %(end_date)s::timestamp);"
MART_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{MART_PROC}(%(mart_month)s);"

default_args = {
    'depends_on_past': False,
    'owner': 'std9_121',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        "std9_121_practice",
        max_active_runs=3,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
) as dag:
    task_start = DummyOperator(task_id="start")

    with TaskGroup("partitions_load") as task_partitions_load_tables:
        for table in PARTITION_LOAD_TABLES:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=DB_CONN,
                                    sql=FACT_TABLE_LOAD_QUERY,
                                    parameters={'ext_tab_name': f'{DB_SCHEMA}.{table}_ext',
                                                'tab_name': f'{DB_SCHEMA}.{table}',
                                                'start_date': START_DATE,
                                                'end_date': END_DATE}
                                   )

    with TaskGroup("full_load") as task_full_insert_tables:
        for table in FULL_LOAD_TABLES:
            task = PostgresOperator(task_id=f"load_table_{table}",
                                    postgres_conn_id=DB_CONN,
                                    sql=MD_TABLE_LOAD_QUERY,
                                    parameters={'ext_tab_name': f'{DB_SCHEMA}.ext_{table}',
                                                'tab_name': f'{DB_SCHEMA}.{table}',
                                                'truncate_tgt': True}
                                   )

    task_load_mart = PostgresOperator(task_id="load_mart",
                                      postgres_conn_id=DB_CONN,
                                      sql=MART_TABLE_LOAD_QUERY,
                                      parameters={'mart_month': '202101'}
                                     )

    task_end = DummyOperator(task_id="end")

    task_start >> task_partitions_load_tables >> task_full_insert_tables >> task_load_mart >> task_end