FROM apache/airflow:2.10.3

USER root

# Установка системных зависимостей
RUN apt-get update && apt-get install -y \
    python3-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Копирование файла requirements.txt
COPY requirements.txt /requirements.txt

# Установка Python пакетов
USER airflow
RUN pip install --no-cache-dir --only-binary :all: -r /requirements.txt