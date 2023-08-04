
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.session import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql import functions as f
from pyspark.sql import SQLContext

# We difnie our app

def main():

    spark = SparkSession \
    .builder \
    .appName("Jobathon Use Case") \
    .config('spark.sql.crossJoin.enabled', 'true') \
    .getOrCreate()

    jfile= './jobathon_sample_data/jobathon_click_data.json'
    data = spark.read.json(jfile)

    data2 = data.withColumn("time_elapsed", data.client_side_data["time_elapsed"]) \
        .withColumn("current_page_url", data.client_side_data["current_page_url"]) \
        .withColumn("time_event", date_format("event_date_time", "yyyyMM").cast('int'))  \
        .withColumn("date_event", date_format("event_date_time", "yyyyMM").cast('int'))  \
        .drop("client_side_data", "event_date_time") 

    data_csv = spark.read.format('com.databricks.spark.csv'). \
    options(header='true', \
    inferschema='true').   \
    load('./jobathon_sample_data/jobathon_login_data.csv', header=True)

    # we merge the two dataset
    data_merge = data2.join(data_csv, data2.session_id==data_csv.session_id, how='inner') #left_outer #full_outer

    data_merge.createOrReplaceTempView("tb3")


    df= spark.sql(
              """
           SELECT
           DISTINCT user_id,
           login_date_time AS current_date,
           browser_id,
           MIN(current_page_url) OVER (ORDER BY user_id DESC) AS current_page_url,
           CASE WHEN time_elapsed IS NULL then 0
                   ELSE 1 END AS logged_in,
           SUM(number_of_pageloads) OVER (PARTITION BY user_id ORDER BY login_date_time) AS number_of_pageloads,
           SUM(number_of_clicks) OVER (PARTITION BY user_id ORDER BY login_date_time) AS number_of_clicks,
           SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY login_date_time) AS user_session_id
           FROM (
              SELECT *,

              CASE WHEN event_type = 'pageload' THEN 1 ELSE 0 END AS number_of_pageloads,
              CASE WHEN event_type = 'clicks' THEN 1 ELSE 0 END AS number_of_clicks,
              CASE WHEN time_elapsed >= (60 * 10)
                 OR time_elapsed IS NULL
               THEN 1 ELSE 0 END AS is_new_session
              FROM (
              SELECT *,
                    user_id, 
                    time_elapsed,
                    login_date_time,
                    LAG(login_date_time,1) OVER
                              (PARTITION BY user_id ORDER BY login_date_time) AS last_event
                    FROM tb3 
              ))

              """)
    df.show(2)



if __name__=="__main__":
    main()