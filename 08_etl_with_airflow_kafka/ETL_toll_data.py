# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'baodkism learn',
    'start_date': days_ago(0),
    'email': ['baodkism+learn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# defining the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)
# define the tasks
dump_folder = '/home/project/airflow/dags/finalassignment'
# define the 'unzip_data' task
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command=f'cd {dump_folder} && tar -xzf tolldata.tgz',
    dag=dag,
)
# define the 'extract_data_from_csv' task
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command=f'cd {dump_folder} && cut -d"," -f1-4 -s vehicle-data.csv > csv_data.csv',
    dag=dag,
)
# define the 'extract_data_from_tsv' task
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command=f'cd {dump_folder} && cut -f5-7 tollplaza-data.tsv --output-delimiter=","  > tsv_data.csv',
    dag=dag,
)
# define the 'extract_data_from_fixed_width' task
extract_data_from_fixed_width = BashOperator(
    task_id='extract_data_from_fixed_width',
    bash_command=f'cd {dump_folder} && cut -c59-61,63-67 --output-delimiter="," payment-data.txt > fixed_width_data.csv',
    dag=dag,
)
# define the 'consolidate_data' task
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command=f'cd {dump_folder} && paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv | tr -d "\r" > extracted_data.csv',
    dag=dag,
)
# define the 'transform_data' task
transform_data = BashOperator(
    task_id='transform_data',
    bash_command=f'cd {dump_folder} && paste -d"," <(cut -d"," -f-3 extracted_data.csv) <(cut -d"," -f4 extracted_data.csv | tr "[:lower:]" "[:upper:]") <(cut -d"," -f5- extracted_data.csv) > transformed_data.csv',
    dag=dag,
)
# task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data