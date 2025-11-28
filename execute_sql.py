import os
from google.cloud import bigquery

# ====== 設定區塊 ======
PROJECT_ID = 'lofty-ivy-473007-k5'
SQL_FILE_PATH = 'transform.sql'
FINAL_TABLE_ID = 'weather_data_lake.daily_weather_report'

def run_bigquery_transform():
    """
    讀取 SQL 檔案並執行 BigQuery 轉換作業。
    """
    print("--- 1. 初始化 BigQuery 客戶端 ---")
    client = bigquery.Client(project=PROJECT_ID)
    
    # 檢查 SQL 檔案是否存在
    if not os.path.exists(SQL_FILE_PATH):
        print(f"錯誤：找不到 SQL 檔案 {SQL_FILE_PATH}")
        return

    # 讀取 SQL 查詢
    with open(SQL_FILE_PATH, 'r', encoding='utf-8') as file:
        sql_query = file.read()

    print(f"--- 2. 開始執行 SQL 轉換作業，寫入 {FINAL_TABLE_ID} ---")

    # bigquery.Client.query() 函數能執行SQL查詢
    query_job = client.query(sql_query)
    
    # 等待任務完成
    query_job.result()  
    
    print(f"轉換任務完成！已成功更新資料表 {FINAL_TABLE_ID}")

# 執行主要函式
if __name__ == '__main__':
    run_bigquery_transform()
	