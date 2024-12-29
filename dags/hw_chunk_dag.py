from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
from scripts.transform_script import transform  # Импортируем функцию из transform_script.py

# Установим базовые параметры DAG
default_args = {
    'owner': 'Alexey Hokkonen',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'execution_timeout': timedelta(minutes=30),  # Увеличиваем лимит времени
}

# Размер чанка
CHUNK_SIZE = 100_000  # Количество строк в одном чанке

# Функция для извлечения данных по частям
def extract_data_chunked(**kwargs):
    file_path = 'dags/data/profit_table.csv'
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} не найден.")

    # Сохраняем путь к промежуточным чанкам
    chunk_dir = 'dags/data/chunks'
    os.makedirs(chunk_dir, exist_ok=True)

    chunk_count = 0
    for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
        chunk_file = os.path.join(chunk_dir, f'chunk_{chunk_count}.csv')
        chunk.to_csv(chunk_file, index=False)
        chunk_count += 1

    kwargs['ti'].xcom_push(key='chunk_dir', value=chunk_dir)
    kwargs['ti'].xcom_push(key='chunk_count', value=chunk_count)

# Функция для трансформации данных по частям
def transform_data_chunked(**kwargs):
    chunk_dir = kwargs['ti'].xcom_pull(key='chunk_dir', task_ids='extract_data')
    chunk_count = kwargs['ti'].xcom_pull(key='chunk_count', task_ids='extract_data')

    if not chunk_dir or not chunk_count:
        raise ValueError("Нет данных для трансформации.")

    transformed_dir = 'dags/data/transformed_chunks'
    os.makedirs(transformed_dir, exist_ok=True)

    for i in range(chunk_count):
        chunk_file = os.path.join(chunk_dir, f'chunk_{i}.csv')
        if not os.path.exists(chunk_file):
            raise FileNotFoundError(f"Чанк {chunk_file} не найден.")

        # Читаем чанк
        chunk = pd.read_csv(chunk_file)

        # Применяем трансформацию
        date = '2024-03-01'
        transformed_chunk = transform(chunk, date)

        # Сохраняем трансформированный чанк
        transformed_file = os.path.join(transformed_dir, f'transformed_chunk_{i}.csv')
        transformed_chunk.to_csv(transformed_file, index=False)

    kwargs['ti'].xcom_push(key='transformed_dir', value=transformed_dir)

# Функция для загрузки данных по частям
def load_data_chunked(**kwargs):
    transformed_dir = kwargs['ti'].xcom_pull(key='transformed_dir', task_ids='transform_data')
    if not transformed_dir:
        raise ValueError("Нет данных для загрузки.")

    output_file = 'dags/data/flags_activity.csv'
    combined_data = []

    for transformed_file in os.listdir(transformed_dir):
        file_path = os.path.join(transformed_dir, transformed_file)
        chunk = pd.read_csv(file_path)
        combined_data.append(chunk)

    # Объединяем все трансформированные чанки
    final_data = pd.concat(combined_data)

    # Сохраняем результат
    if os.path.exists(output_file):
        existing_data = pd.read_csv(output_file)
        final_data = pd.concat([existing_data, final_data])

    final_data.to_csv(output_file, index=False, mode='w', header=True)

# Создаём DAG
with DAG(
    'hw-airflow-dag-chunked',
    default_args=default_args,
    description='DAG для расчета активности клиентов с обработкой чанков',
    schedule_interval='0 0 5 * *',  # Запуск 5-го числа каждого месяца
    start_date=datetime(2023, 10, 1),
    catchup=False,
    tags=['ETL', 'client_activity', 'chunk'],
) as dag:

    extract_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_chunked,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_chunked,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_chunked,
        provide_context=True
    )

    extract_task >> transform_task >> load_task
