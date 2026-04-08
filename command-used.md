Sqoop Import command used for importing table from RDS to HDFS:

sqoop import \
--connect jdbc:mysql://upgraddetest.cyaielc9bmnf.us-east-1.rds.amazonaws.com/testdatabase \
--username student \
--password STUDENT123 \
--table SRC_ATM_TRANS \
--target-dir /user/hadoop/atm_raw \
--as-textfile \
-m 1

Command used to see the list of imported data in HDFS:

hdfs dfs -ls /user/hadoop/atm_raw
hdfs dfs -head /user/hadoop/atm_raw/part-m-00000


>>> from pyspark.sql.types import *
>>> from pyspark.sql.functions import *
>>>
>>> atm_schema = StructType([
StructField("year", IntegerType(), True),
StructField("month", StringType(), True),
StructField("day", IntegerType(), True),
StructField("weekday", StringType(), True),
StructField("hour", IntegerType(), True),
...
StructField("atm_status", StringType(), True),
StructField("atm_id", StringType(), True),
StructField("atm_manufacturer", StringType(), True),
StructField("atm_location", StringType(), True),
StructField("atm_street_name", StringType(), True),
StructField("atm_street_number", IntegerType(), True),
StructField("atm_zipcode", IntegerType(), True),
StructField("atm_lat", DoubleType(), True),
StructField("atm_lon", DoubleType(), True),
...
StructField("currency", StringType(), True),
StructField("card_type", StringType(), True),
StructField("transaction_amount", IntegerType(), True),
StructField("service", StringType(), True),
...
StructField("message_code", StringType(), True),
StructField("message_text", StringType(), True),
...
StructField("weather_lat", DoubleType(), True),
StructField("weather_lon", DoubleType(), True),
StructField("weather_city_id", IntegerType(), True),
StructField("weather_city_name", StringType(), True),
StructField("temp", DoubleType(), True),
StructField("pressure", IntegerType(), True),
StructField("humidity", IntegerType(), True),
StructField("wind_speed", IntegerType(), True),
StructField("wind_deg", IntegerType(), True),
StructField("rain_3h", DoubleType(), True),
StructField("clouds_all", IntegerType(), True),
StructField("weather_id", IntegerType(), True),
StructField("weather_main", StringType(), True),
StructField("weather_description", StringType(), True)
... ])
>>> atm_df = spark.read \
.schema(atm_schema) \
.csv("hdfs:///user/hadoop/atm_raw")

atm_df.show(5, truncate=False)
+----+-------+---+-------+----+----------+------+----------------+------------+-------------------+-----------------+-----------+-------+-------+--------+----------+------------------+----------+------------+------------+-----------+-----------+---------------+-----------------+------+--------+--------+----------+--------+-------+----------+----------+------------+-----------------------+
|year|month  |day|weekday|hour|atm_status|atm_id|atm_manufacturer|atm_location|atm_street_name    |atm_street_number|atm_zipcode|atm_lat|atm_lon|currency|card_type |transaction_amount|service   |message_code|message_text|weather_lat|weather_lon|weather_city_id|weather_city_name|temp  |pressure|humidity|wind_speed|wind_deg|rain_3h|clouds_all|weather_id|weather_main|weather_description    |
+----+-------+---+-------+----+----------+------+----------------+------------+-------------------+-----------------+-----------+-------+-------+--------+----------+------------------+----------+------------+------------+-----------+-----------+---------------+-----------------+------+--------+--------+----------+--------+-------+----------+----------+------------+-----------------------+
|2017|January|1  |Sunday |0   |Active    |1     |NCR             |NÃƒÂ¦stved  |Farimagsvej        |8                |4700       |55.233 |11.763 |DKK     |MasterCard|5643            |Withdrawal|null        |null        |55.23      |11.761     |2616038        |Naestved         |281.15|1014    |87      |7         |260     |0.215  |92|500       |Rain        |light rain             |
|2017|January|1  |Sunday |0   |Inactive  |2     |NCR             |Vejgaard    |Hadsundvej         |20               |9000       |57.043 |9.95   |DKK     |MasterCard|1764            |Withdrawal|null        |null        |57.048     |9.935      |2616235        |NÃƒÂ¸rresundby   |280.64|1020    |93      |9         |250     |0.59   |92|500       |Rain        |light rain             |
|2017|January|1  |Sunday |0   |Inactive  |2     |NCR             |Vejgaard    |Hadsundvej         |20               |9000       |57.043 |9.95   |DKK     |VISA      |1891            |Withdrawal|null        |null        |57.048     |9.935      |2616235        |NÃƒÂ¸rresundby   |280.64|1020    |93      |9         |250     |0.59   |92|500       |Rain        |light rain             |
|2017|January|1  |Sunday |0   |Inactive  |3     |NCR             |Ikast       |RÃƒÂ¥dhusstrÃƒÂ¦det|12               |7430       |56.139 |9.154  |DKK     |VISA      |4166            |Withdrawal|null        |null        |56.139     |9.158      |2619426        |Ikast            |281.15|1011    |100     |6         |240     |0.0    |75|300       |Drizzle     |light intensity drizzle|
|2017|January|1  |Sunday |0   |Active    |4     |NCR             |Svogerslev  |BrÃƒÂ¸nsager       |1                |4000       |55.634 |12.018 |DKK     |MasterCard|5153            |Withdrawal|null        |null        |55.642     |12.08      |2614481        |Roskilde         |280.61|1014    |87      |7         |260     |0.0    |88|701       |Mist        |mist                   |
+----+-------+---+-------+----+----------+------+----------------+------------+-------------------+-----------------+-----------+-------+-------+--------+----------+------------------+----------+------------+------------+-----------+-----------+---------------+-----------------+------+--------+--------+----------+--------+-------+----------+----------+------------+-----------------------+
only showing top 5 rows

>>> dim_location = atm_df.select(
col("atm_location").alias("location"),
col("atm_street_name"),
col("atm_street_number"),
col("atm_zipcode"),
col("atm_lat").alias("lat"),
col("atm_lon").alias("lon")
... ).dropDuplicates() \
...  .withColumn("location_id", monotonically_increasing_id())

>>> dim_atm_base = atm_df.select(
"atm_id",
"atm_manufacturer",
"atm_location"
... ).dropDuplicates(["atm_id"])
>>> atm_location_lookup = dim_location \
.select("location_id", "location") \
.dropDuplicates(["location"])
>>> dim_atm = dim_atm_base \
.join(
    atm_location_lookup,
    dim_atm_base.atm_location == atm_location_lookup.location,
    "left"
) \
.select(
    col("atm_id").cast("int"),
    "atm_number",
    "atm_manufacturer",
    col("location_id").alias("atm_location_id")
)

>>> from pyspark.sql.functions import to_timestamp, concat_ws, lit
>>>
>>> dim_date = atm_df.select(
concat_ws(" ",
    col("year"),
    col("month"),
    col("day"),
    col("hour")
).alias("full_date_time"),
"year", "month", "day", "hour", "weekday"
... ).dropDuplicates() \
...  .withColumn("date_id", monotonically_increasing_id())

>>> dim_card_type = atm_df.select("card_type") \
.dropDuplicates() \
.withColumn("card_type_id", monotonically_increasing_id())

>>> weather_location_lookup = dim_location \
...     .select(
...         "location_id",
...         "location",
...         "lat",
...         "lon"
...     ) \
...     .dropDuplicates(["lat", "lon"])
>>> weather_location_lookup = dim_location \
...     .select("location_id", "lat", "lon") \
...     .dropDuplicates(["lat", "lon"])
>>> fact_atm_trans = atm_df \
...     .join(dim_atm, "atm_id") \
...     .join(dim_card_type, "card_type") \
...     .join(dim_date, ["year", "month", "day", "hour", "weekday"]) \
...     .join(
...         weather_location_lookup,
...         (atm_df.weather_lat == weather_location_lookup.lat) &
...         (atm_df.weather_lon == weather_location_lookup.lon),
...         "left"
...     ) \
...     .select(
...         monotonically_increasing_id().alias("trans_id"),
...         col("atm_id").cast("int"),
...         col("location_id").alias("weather_loc_id"),
...         col("date_id"),
...         col("card_type_id"),
...         col("atm_status"),
...         col("currency"),
...         col("service"),
...         col("transaction_amount"),
...         col("message_code"),
...         col("message_text"),
...         col("rain_3h"),
...         col("clouds_all"),
...         col("weather_id"),
...         col("weather_main"),
...         col("weather_description")
...     )

>>> dim_location.write.mode("overwrite").parquet("s3://mike-upgrad-etl-project/dim_location/")
>>> dim_atm.write.mode("overwrite").parquet("s3://mike-upgrad-etl-project/dim_atm/")
>>> dim_date.write.mode("overwrite").parquet("s3://mike-upgrad-etl-project/dim_date/")
>>> dim_card_type.write.mode("overwrite").parquet("s3://mike-upgrad-etl-project/dim_card_type/")
>>> fact_atm_trans.write.mode("overwrite").parquet("s3://mike-upgrad-etl-project/fact_atm_trans/")

>>> dim_location.show(5)
+-------------+----------------+-----------------+-----------+------+------+-----------+
|     location| atm_street_name|atm_street_number|atm_zipcode|   lat|   lon|location_id|
+-------------+----------------+-----------------+-----------+------+------+-----------+
|Frederikshavn|    Danmarksgade|               48|       9900|57.441|10.537|          0|
|       Aarhus|  SÃƒÂ¸nder Alle|               11|       8000|56.153|10.206|          1|
|  HÃƒÂ¸jbjerg|  Rosenvangsalle|              194|       8270|56.119|10.192|          2|
|     Roskilde|KÃƒÂ¸benhavnsvej|               65|       4000|55.642|12.106|          3|
|   Middelfart|         Brogade|                9|       5500|55.507| 9.727|          4|
+-------------+----------------+-----------------+-----------+------+------+-----------+
only showing top 5 rows

>>> dim_atm.show(5)
[Stage 69:>                 (0 + 4) / 4][Stage 70:>                 (0 + 0) / 4]
+------+----------+----------------+---------------+
|atm_id|atm_number|atm_manufacturer|atm_location_id|
+------+----------+----------------+---------------+
|    42|        42|             NCR|              9|
|    43|        43|             NCR|             31|
|    85|        85| Diebold Nixdorf|             15|
|    22|        22|             NCR|              3|
|    78|        78| Diebold Nixdorf|              7|
+------+----------+----------------+---------------+
only showing top 5 rows

>>>
>>> dim_date.show(5)
+----------------+----+-----+---+----+---------+-------+
|  full_date_time|year|month|day|hour|  weekday|date_id|
+----------------+----+-----+---+----+---------+-------+
|  2017 June 21 8|2017| June| 21|   8|Wednesday|      0|
|    2017 May 5 4|2017|  May|  5|   4|   Friday|      1|
| 2017 June 17 11|2017| June| 17|  11| Saturday|      2|
| 2017 June 26 11|2017| June| 26|  11|   Monday|      3|
|2017 April 18 15|2017|April| 18|  15|  Tuesday|      4|
+----------------+----+-----+---+----+---------+-------+
only showing top 5 rows

>>> fact_atm_trans.show(5)
+--------+------+--------------+-------+------------+----------+--------+----------+------------------+------------+------------+-------+----------+----------+------------+-------------------+
|trans_id|atm_id|weather_loc_id|date_id|card_type_id|atm_status|currency|   service|transaction_amount|message_code|message_text|rain_3h|clouds_all|weather_id|weather_main|weather_description|
+--------+------+--------------+-------+------------+----------+--------+----------+------------------+------------+------------+-------+----------+----------+------------+-------------------+
|       0|    42|             8|   3769|           1|    Active|     DKK|Withdrawal|              8965|        null|        null|    0.0|         0|       800|       Clear|       Sky is Clear|
|       1|    42|             8|   3769|           4|    Active|     DKK|Withdrawal|              5235|        null|        null|    0.0|         0|       800|       Clear|       Sky is Clear|
|       2|    42|             8|   3769|           4|    Active|     DKK|Withdrawal|              8379|        null|        null|    0.0|         0|       800|       Clear|       Sky is Clear|
|       3|    42|             8|   3769|          10|    Active|     DKK|Withdrawal|              4867|        null|        null|    0.0|         0|       800|       Clear|       Sky is Clear|
|       4|    42|             8|   3990|          10|    Active|     DKK|Withdrawal|              7244|        null|        null|    0.0|         0|       800|       Clear|       Sky is Clear|
+--------+------+--------------+-------+------------+----------+--------+----------+------------------+------------+------------+-------+----------+----------+------------+-------------------+
only showing top 5 rows

>>>
>>> atm_df.count()
2468572
>>> fact_atm_trans.count()
2468572
>>> fact_atm_trans.count() == atm_df.count()
True
>>> dim_location.count()
109
>>> dim_card_type.count()
12
>>> dim_atm.count()
113
>>> dim_date.count()
8685