from __future__ import annotations
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# 為了讓 Airflow Worker 能夠執行我們的 Python 腳本，我們將直接在 BashOperator 中呼叫它們。

# ====== DAG 設定 ======
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_data_elt_pipeline',
    default_args=default_args,
    description='End-to-end ELT pipeline for weather data using GCS and BigQuery.',
    # 設定為每天執行一次
    schedule='0 6 * * *', # 每天早上 6:00 UTC 執行
    start_date=datetime(2025, 11, 28),
    catchup=False,
    tags=['data-engineering', 'gcp', 'elt'],
) as dag:
    
    # 任務 1: 資料提取 (E) 與 GCS 載入 (L - GCS)
    # 此任務執行 extract.py，將資料從 API 轉換為 NDJSON 並上傳至 GCS。
    extract_to_gcs = BashOperator(
        task_id='1_extract_data_to_gcs',
        # Airflow Worker 必須能夠找到並執行 extract.py 腳本。
        # 由於 extract.py 和 load_bq.py 暫時放在 dags/ 資料夾中，我們直接呼叫它們。
        bash_command='python $AIRFLOW_HOME/data/extract.py',
    )
    
    # 任務 2: BigQuery 載入 (L - BQ) 與 SQL 轉換 (T)
    # 此任務依序執行 load_bq.py 和 execute_sql.py，完成載入和轉換。
    load_and_transform = BashOperator(
        task_id='2_load_and_transform_data',
        # 我們使用分號 ; 來串連多個指令
        bash_command='''
            python $AIRFLOW_HOME/data/load_bq.py;
            python $AIRFLOW_HOME/data/execute_sql.py;
        ''',
    )

    # 建立任務依賴關係: 提取必須在轉換之前完成
    extract_to_gcs >> load_and_transform
    