import airflow
import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.postgres_operator import PostgresOperator


FILENAME = "/opt/airflow/dags/new_data.csv"
USER_INSERT_SQL = "user_insert.sql"

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': None,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1),
}

with DAG(
    dag_id='sensors_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
) as sensors_dag:

    file_sensor = FileSensor(
        task_id = "file_sensor",
        filepath= FILENAME,
        poke_interval = 1,
        timeout = 10
    )

    fetch_users = BashOperator(
        task_id='get_spreadsheet',
        dag=sensors_dag,
        bash_command=f'curl "https://randomuser.me/api/?results=5&format=csv" --output {FILENAME}',
    )

    create_user_table = PostgresOperator(
        task_id="create_user_table",
        postgres_conn_id="postgres_default",
        sql="""
            CREATE TABLE IF NOT EXISTS users (
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            age INT NOT NULL);
          """,
        trigger_rule='none_failed',
        autocommit=True,
    )

    insert_data = PostgresOperator(
        task_id="insert_data",
        postgres_conn_id="postgres_default",
        sql=USER_INSERT_SQL,
        trigger_rule='none_failed',
        autocommit=True,
    )

    start_task = DummyOperator(
        task_id = "start",
    )

    def _create_sql():
        import pandas as pd
        df = pd.read_csv(FILENAME)
        df = df[['name.first', 'name.last', 'dob.age']]
        df = df.rename(columns = {
            'name.first': 'first_name', 
            'name.last': 'last_name', 
            'dob.age': 'age'
        })
        df.to_csv(path_or_buf=FILENAME)
        with open(f"/opt/airflow/dags/{USER_INSERT_SQL}", "w") as f:
            for _, row in df.iterrows():
                f.write(
                    "INSERT INTO users VALUES ("
                    f"'{row['first_name']}', '{row['last_name']}', '{row['age']}'"
                    ");\n"
                )

    create_sql = PythonOperator(
        task_id='clean_csv',
        python_callable=_create_sql,
        depends_on_past=False,
    )

    def _mean_change():
        file = open('avg', 'r')
        mean = int(file.readline())

    compute_mean = BranchPythonOperator(
        task_id='compute_mean',
        python_callable=_mean_change,
        depends_on_past=False,
    )

    end_tasks = DummyOperator(
        trigger_rule='all_success',
        task_id = "finish",
    )

start_task >> fetch_users >> file_sensor >> create_sql >> create_user_table >> insert_data>> end_tasks


