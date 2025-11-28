import requests
import json
from datetime import datetime
# Google Cloud SDK 將自動使用 GOOGLE_APPLICATION_CREDENTIALS 變數
from google.cloud import storage 

# ====== 設定區塊 ======

# 紐約市經緯度
LATITUDE = 40.71
LONGITUDE = -74.01

# 您的儲存桶名稱
GCS_BUCKET_NAME = 'de_project_weather_data_sj'

# API 請求參數
API_URL = 'https://api.open-meteo.com/v1/forecast'
PARAMS = {
    'latitude': LATITUDE,
    'longitude': LONGITUDE,
    'daily': ['temperature_2m_max', 'temperature_2m_min', 'weather_code'],
    'timezone': 'America/New_York',
    'forecast_days': 7 
}

# ====== 主要函式 ======

def run_extraction():
    """
    執行資料提取與上傳的主流程：
    1. 呼叫 Open-Meteo API 提取資料。
    2. 將原始 JSON 資料儲存為 GCS 上的檔案。
    """
    print("--- 1. 開始呼叫 Open-Meteo API ---")
    try:
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status() 
        data = response.json()
        print("API 呼叫成功，已接收資料。")
    except requests.exceptions.RequestException as e:
        print(f"API 呼叫失敗或 HTTP 錯誤: {e}")
        return

    print("--- 2. 開始轉換資料結構 ---")
    
    # 執行關鍵的轉換：將巢狀陣列展開成多個記錄
    # 獲取日期的數量
    num_days = len(data['daily']['time'])
    
    # 創建一個列表，包含每天的獨立 JSON 記錄
    records = []
    for i in range(num_days):
        record = {
            'metadata_latitude': data.get('latitude'),
            'metadata_longitude': data.get('longitude'),
            'date': data['daily']['time'][i],
            'temp_max': data['daily']['temperature_2m_max'][i],
            'temp_min': data['daily']['temperature_2m_min'][i],
            'weather_code': data['daily']['weather_code'][i],
            'extraction_timestamp': datetime.now().isoformat()
        }
        records.append(record)

    # 將記錄列表轉換為 NEWLINE_DELIMITED_JSON 字串
    # 每個記錄佔一行，用於 BigQuery 載入
    json_lines = "\n".join(json.dumps(record) for record in records)

    print(f"已展開 {len(records)} 個獨立記錄。")

    print("--- 3. 開始上傳資料至 Cloud Storage ---")
    
    # 建立 GCS 檔案名稱
    #    now = datetime.now().strftime("%Y%m%d_%H%M%S")
    #    GCS_FILE_NAME = f"raw_data/{now}_weather_data.json"

    # 建立 GCS 檔案名稱 (使用固定名稱，讓每次載入覆蓋它)
    GCS_FILE_NAME = "raw_data/latest_weather_data.json"

    try:
        storage_client = storage.Client() 
        bucket = storage_client.bucket(GCS_BUCKET_NAME)
        blob = bucket.blob(GCS_FILE_NAME)
        
        # 關鍵上傳：上傳 NEWLINE_DELIMITED_JSON 字串
        blob.upload_from_string(
            data=json_lines, # 上傳轉換後的 NEWLINE_DELIMITED_JSON 內容
            content_type='application/json'
        )
        
        print(f"資料成功上傳到 GCS: gs://{GCS_BUCKET_NAME}/{GCS_FILE_NAME}")
        
    except Exception as e:
        print(f"上傳至 GCS 失敗: {e}")
        
# 執行主要函式
if __name__ == '__main__':
    run_extraction() 
    