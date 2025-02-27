from datetime import datetime, timedelta, date
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

# Параметры подключения к базе данных Greenplum
DB_CONN = "gp_std9_121"
DB_SCHEMA = 'std9_121'

# Параметры для загрузки справочников в хранилище
MD_LOAD = ['f_create_gpfdist_table', 'f_full_load']
FULL_LOAD_TABLES = ['coupons', 'stores', 'promos', 'promo_types']
FULL_LOAD_FILES = {'coupons': 'coupons', 'stores': 'stores', 'promos': 'promos', 'promo_types': 'promo_types'}
CREATE_GPFDIST_TABLE_QUERY = f"select {DB_SCHEMA}.{MD_LOAD[0]}(%(tab_name)s, %(file)s);"
MD_TABLE_LOAD_QUERY = f"select {DB_SCHEMA}.{MD_LOAD[1]}(%(ext_tab_name)s, %(tab_name)s, %(truncate_tgt)s);"

# Параметры для загрузки таблиц фактов в хранилище
FACT_LOAD = ['f_create_pxf_table', 'f_load_delta_partitions', 'f_full_load']
FACT_LOAD_TABLES = ['traffic', 'bills_head', 'bills_item']
START_DATE, END_DATE = '2021-01-01', '2021-02-28'
CREATE_PXF_TABLE_QUERY = f"select {DB_SCHEMA}.{FACT_LOAD[0]}(%(tab_name)s, %(pxf_tab_name)s, %(user_id)s, %(pass)s);"
TABLE_INCREMENTAL_LOAD_QUERY = f"select {DB_SCHEMA}.{FACT_LOAD[1]}(%(ext_tab_name)s, %(tab_name)s, %(partition_key)s, %(start_date)s::timestamp, %(end_date)s::timestamp);"
TABLE_FULL_LOAD_QUERY = f"select {DB_SCHEMA}.{FACT_LOAD[2]}(%(ext_tab_name)s, %(tab_name)s, %(truncate_tgt)s);"

# Запросы для преобразования и перемещения таблиц из RAW в ODS слой хранилища
# TRANSFER_TO_ODS_TABLES = ['coupons', 'promos', 'traffic']
RAW_TO_ODS_FULL_LOAD_COUPONS = f"select {DB_SCHEMA}.f_full_load_coupons('{DB_SCHEMA}.coupons', '{DB_SCHEMA}.ods_coupons', true)"
RAW_TO_ODS_FULL_LOAD_PROMOS = f"select {DB_SCHEMA}.f_full_load_promos('{DB_SCHEMA}.promos', '{DB_SCHEMA}.ods_promos', true)"
RAW_TO_ODS_INCREMENTAL_LOAD_TRAFFIC = f"select {DB_SCHEMA}.f_load_delta_partitions_traffic('{DB_SCHEMA}.traffic', '{DB_SCHEMA}.ods_traffic', '2021-01-01', '2021-02-28')"

# Запросы для преобразования и перемещения таблиц из ODS в DDS слой хранилища
ODS_TO_DDS_FULL_LOAD_COUPONS = f"select {DB_SCHEMA}.f_full_load_dds_coupons('{DB_SCHEMA}.ods_coupons', '{DB_SCHEMA}.dds_coupons', true)"
ODS_TO_DDS_INCREMENTAL_LOAD_BILLS = f"select {DB_SCHEMA}.f_load_delta_partitions_dds_bills('{DB_SCHEMA}.bills_item', '{DB_SCHEMA}.dds_bills', 'bi.calday', '2021-01-01', '2021-02-28')"

# Запрос для расчета витрины
SALES_MART_CALCULATION = f"select {DB_SCHEMA}.f_load_sales_mart(%(start_date)s, %(end_date)s);"
SALES_BY_DAY_MART_CALCULATION = f"select {DB_SCHEMA}.f_load_sales_mart(%(date)s);"


default_args = {
    'depends_on_past': False,
    'owner': 'std9_121',
    'start_date': datetime(2025, 2, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

with DAG(
        dag_id="std9_121_final_project",
        max_active_runs=3,
        schedule_interval=None,
        default_args=default_args,
        catchup=False,
) as dag:
    task_start = DummyOperator(task_id="start")

    # Группа задач для загрузки таблиц справочников
    with TaskGroup("master_data_load_raw") as task_master_data_tables_load:
        for table in FULL_LOAD_TABLES:
            task_pt_1 = PostgresOperator(task_id=f"create_table_{table}_ext",
                                         postgres_conn_id=DB_CONN,
                                         sql=CREATE_GPFDIST_TABLE_QUERY,
                                         parameters={'tab_name': f'{DB_SCHEMA}.{table}',
                                                     'file': FULL_LOAD_FILES[table]}
                                         )

            task_pt_2 = PostgresOperator(task_id=f"load_table_{table}",
                                         postgres_conn_id=DB_CONN,
                                         sql=MD_TABLE_LOAD_QUERY,
                                         parameters={'ext_tab_name': f'{DB_SCHEMA}.{table}_ext',
                                                     'tab_name': f'{DB_SCHEMA}.{table}',
                                                     'truncate_tgt': True}
                                         )
            task_pt_1 >> task_pt_2

    # Группа задач для загрузки таблиц фактов
    with TaskGroup("fact_load_raw") as task_fact_tables_load:
        for table in FACT_LOAD_TABLES:
            task_pt_1 = PostgresOperator(task_id=f"create_table_{table}_ext",
                                         postgres_conn_id=DB_CONN,
                                         sql=CREATE_PXF_TABLE_QUERY,
                                         parameters={'tab_name': f'{table}',
                                                     'pxf_tab_name': f'gp.{table}',
                                                     'user_id': 'intern',
                                                     'pass': 'intern'}
                                         )
            if table != 'traffic':
                task_pt_2 = PostgresOperator(task_id=f"load_table_{table}",
                                             postgres_conn_id=DB_CONN,
                                             sql=TABLE_INCREMENTAL_LOAD_QUERY,
                                             parameters={'ext_tab_name': f'{DB_SCHEMA}.{table}_ext',
                                                         'tab_name': f'{DB_SCHEMA}.{table}',
                                                         'partition_key': 'calday',
                                                         'start_date': START_DATE,
                                                         'end_date': END_DATE}
                                             )
            else:
                task_pt_2 = PostgresOperator(task_id=f"load_table_{table}",
                                             postgres_conn_id=DB_CONN,
                                             sql=TABLE_FULL_LOAD_QUERY,
                                             parameters={'ext_tab_name': f'{DB_SCHEMA}.{table}_ext',
                                                         'tab_name': f'{DB_SCHEMA}.{table}',
                                                         'truncate_tgt': True}
                                             )

            task_pt_1 >> task_pt_2

    # Группа задач для выгрузки из слоя сырых данных (Raw Data Layer) в ODS (Operational Data Store) слой
    with TaskGroup("raw_to_ods_load") as task_transfer_tables_to_ods:
        task_pt_1 = PostgresOperator(task_id="full_load_coupons_to_ods",
                                     postgres_conn_id=DB_CONN,
                                     sql=RAW_TO_ODS_FULL_LOAD_COUPONS
                                     )

        task_pt_2 = PostgresOperator(task_id="full_load_promos_to_ods",
                                     postgres_conn_id=DB_CONN,
                                     sql=RAW_TO_ODS_FULL_LOAD_PROMOS
                                     )

        task_pt_3 = PostgresOperator(task_id="incremental_load_traffic_to_ods",
                                     postgres_conn_id=DB_CONN,
                                     sql=RAW_TO_ODS_INCREMENTAL_LOAD_TRAFFIC
                                     )

        task_pt_1 >> task_pt_2 >> task_pt_3

    # Группа задач для выгрузки из операционного слоя ODS в детальный слой DDS (Detailed Data Store) слой
    with TaskGroup("ods_to_dds_load") as task_transfer_tables_to_dds:
        task_pt_1 = PostgresOperator(task_id="full_load_coupons_to_dds",
                                     postgres_conn_id=DB_CONN,
                                     sql=ODS_TO_DDS_FULL_LOAD_COUPONS
                                     )

        task_pt_2 = PostgresOperator(task_id="incremental_load_bills_to_dds",
                                     postgres_conn_id=DB_CONN,
                                     sql=ODS_TO_DDS_INCREMENTAL_LOAD_BILLS
                                     )

        task_pt_1 >> task_pt_2

    # Расчет витрины
    task_load_mart = PostgresOperator(task_id="load_mart",
                                      postgres_conn_id=DB_CONN,
                                      sql=SALES_MART_CALCULATION,
                                      parameters={'start_date': START_DATE,
                                                  'end_date': END_DATE}
                                      )

    task_end = DummyOperator(task_id="end")

    task_start >> task_master_data_tables_load >> task_fact_tables_load >> task_transfer_tables_to_ods >> task_transfer_tables_to_dds >> task_load_mart >> task_end

