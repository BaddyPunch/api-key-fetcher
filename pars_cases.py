from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, date_format
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql.types import StructType, StructField, StringType
from connect import get_apikey
import requests


spark = SparkSession.builder \
    .appName("CaseDataProcessing") \
    .getOrCreate()

schema = StructType([
    StructField("caseId", StringType(), True),
])


def get_caseId(file):
    df = spark.read.csv(file, header=False, schema=schema)
    mass = df.rdd.map(lambda row: row[0]).collect()
    return mass


def get_data_filtered(apikey, caseId):
    api_call_uri = f'https://api3.casebook.ru/arbitrage/cases/{caseId}/managersHistory?apikey={apikey}'
    response = requests.get(api_call_uri, timeout=30)

    if response.status_code == 200:
        data = response.json()
        items = data.get('items', [])
        if items:
            # Фильтруем только те элементы, у которых есть дата и она >= 2021 года
            filtered_data = [{"caseId": caseId, "startDate": item.get('startDate')}
                             for item in items
                             if item.get('startDate') and item.get('startDate') >= '2021-01-01']
            return filtered_data
    return []


def filter_and_save_data_parallel(apikey, case_ids, output_file, num_threads=5):
    results = []

    # Параллельная обработка запросов с помощью ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        futures = [executor.submit(get_data_filtered, apikey, caseId) for caseId in case_ids]

        for future in futures:
            result = future.result()
            if result:
                results.extend(result)

    if results:

        df = spark.createDataFrame(results)

        df = df.withColumn("startDate", to_date(col("startDate"), "yyyy-MM-dd'T'HH:mm:ss")) \
            .withColumn("startDate", date_format(col("startDate"), "dd.MM.yyyy"))

        df.coalesce(1).write.csv(output_file, header=True, mode="overwrite")


def main():
    apikey = get_apikey()
    case_ids = get_caseId('caseid/output_file.csv')
    filter_and_save_data_parallel(apikey, case_ids, "filtered_cases.csv", num_threads=10)


if __name__ == '__main__':
    main()
