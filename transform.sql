-- 程式碼用途：使用 CREATE OR REPLACE TABLE AS SELECT (CRTAS) 模式，並使用 CAST 進行類型轉換。

CREATE OR REPLACE TABLE `lofty-ivy-473007-k5.weather_data_lake.daily_weather_report`
AS
SELECT
    t1.extraction_timestamp,
    t1.metadata_latitude AS latitude,
    t1.metadata_longitude AS longitude,
    CAST(t1.date AS DATE) AS forecast_date,  -- 使用 CAST 函式將字串直接轉換為 DATE 類型
    t1.temp_max,
    t1.temp_min,
    t1.weather_code
FROM
    `lofty-ivy-473007-k5.weather_data_lake.raw_forecast_staging` AS t1;
	