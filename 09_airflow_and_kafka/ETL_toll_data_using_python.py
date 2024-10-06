# import the libraries
from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago
import requests
import tarfile
import csv
import shutil

#defining DAG arguments
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'baodkism learn',
    'start_date': days_ago(0),
    'email': ['baodkism+learn'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
# defining the DAG
dag = DAG(
    'ETL_toll_data_py',
    default_args=default_args,
    description='Apache Airflow Final Assignment',
    schedule_interval=timedelta(days=1),
)
# define the input source path
source_url = 'https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Final%20Assignment/tolldata.tgz'
# define destination
dump_folder = '/home/project/airflow/dags/python_etl'

# define python functions
def download_dataset():
    response = requests.get(source_url, stream=True)
    if response.status_code == 200:
        with open(f"{dump_folder}/staging/tolldata.tgz", "wb") as f:
            f.write(response.raw.read())
    else:
        print("Failed to download the file")

def untar_dataset():
    with tarfile.open(f"{dump_folder}/staging/tolldata.tgz", "r:gz") as tar:
        tar.extractall(f"{dump_folder}")

def extract_data_from_csv():
    input_file = f"{dump_folder}/vehicle-data.csv"
    output_file = f"{dump_folder}/csv_data.csv"
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Rowid', 'Timestamp', 'Annymized Vehicle number', 'Vehicle type'])
        for line in infile:
            row = line.split(',')
            writer.writerow(row[0:4])

def extract_data_from_tsv():
    input_file = f"{dump_folder}/tollplaza-data.tsv"
    output_file = f"{dump_folder}/tsv_data.csv"
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Number of axles', 'Tollplaza id', 'Tollplaza code'])
        for line in infile:
            row = line.split('\t')
            writer.writerow(row[0:3])

def extract_data_from_fixed_width():
    input_file = f"{dump_folder}/payment-data.txt"
    output_file = f"{dump_folder}/fixed_width_data.csv"
    with open(input_file, "r") as infile, open(output_file, "w") as outfile:
        writer = csv.writer(outfile)
        writer.writerow(['Type of Payment code', 'Vehicle Code'])
        for line in infile:
            writer.writerow([line[0:6].strip(), line[6:12].strip()])

def consolidate_data():
    csv_file = f"{dump_folder}/csv_data.csv"
    tsv_file = f"{dump_folder}/tsv_data.csv"
    fixed_width_file = f"{dump_folder}/fixed_width_data.csv"
    output_file = f"{dump_folder}/extracted_data.csv"

    with open(csv_file, 'r') as csv_in, open(tsv_file, 'r') as tsv_in, open(fixed_width_file, 'r') as fixed_in, open(output_file, 'w') as out_file:
        csv_reader = csv.reader(csv_in)
        tsv_reader = csv.reader(tsv_in)
        fixed_reader = csv.reader(fixed_in)
        writer = csv.writer(out_file)
        writer.writerow(['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type', 'Number of axles', 'Tollplaza id', 'Tollplaza code', 'Type of Payment code', 'Vehicle Code'])
        next(csv_reader)
        next(tsv_reader)
        next(fixed_reader)
        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            writer.writerow(csv_row + tsv_row + fixed_row)


def transform_data():
    input_file = f"{dump_folder}/extracted_data.csv"
    output_file = f"{dump_folder}/transformed_data.csv"
    
    with open(input_file, 'r') as infile, open(output_file, 'w') as outfile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            row['Vehicle type'] = row['Vehicle type'].upper()
            writer.writerow(row)

# define the tasks
# define the 'download_dataset" task
download_task = PythonOperator(
    task_id='download_task',
    python_callable=download_dataset,
    dag=dag,
)
# define the 'untar_data' task
untar_data_task = PythonOperator(
    task_id='untar_data_task',
    python_callable=untar_dataset,
    dag=dag,
)
# define the 'extract_data_from_csv' task
extract_csv_task = PythonOperator(
    task_id='extract_csv_task',
    python_callable=extract_data_from_csv,
    dag=dag,
)
# define the 'extract_data_from_tsv' task
extract_tsv_task = PythonOperator(
    task_id='extract_tsv_task',
    python_callable=extract_data_from_tsv,
    dag=dag,
)
# define the 'extract_data_from_fixed_width' task
extract_fixed_width_task = PythonOperator(
    task_id='extract_fixed_width_task',
    python_callable=extract_data_from_fixed_width,
    dag=dag,
)
# define the 'consolidate_data' task
consolidate_data_task = PythonOperator(
    task_id='consolidate_data_task',
    python_callable=consolidate_data,
    dag=dag,
)
# define the 'transform_data' task
transform_data_task = PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    dag=dag,
)
# task pipeline
download_task >> untar_data_task >> extract_csv_task >> extract_tsv_task >> extract_fixed_width_task >> consolidate_data_task >> transform_data_task