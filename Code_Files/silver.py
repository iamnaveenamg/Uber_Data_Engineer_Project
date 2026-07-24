from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Spark will initialized by Databricks

rides_schema1=StructType([StructField('ride_id', StringType(), True), StructField('confirmation_number', StringType(), True), StructField('passenger_id', StringType(), True), StructField('driver_id', StringType(), True), StructField('vehicle_id', StringType(), True), StructField('pickup_location_id', StringType(), True), StructField('dropoff_location_id', StringType(), True), StructField('vehicle_type_id', LongType(), True), StructField('vehicle_make_id', LongType(), True), StructField('payment_method_id', LongType(), True), StructField('ride_status_id', LongType(), True), StructField('pickup_city_id', LongType(), True), StructField('dropoff_city_id', LongType(), True), StructField('cancellation_reason_id', LongType(), True), StructField('passenger_name', StringType(), True), StructField('passenger_email', StringType(), True), StructField('passenger_phone', StringType(), True), StructField('driver_name', StringType(), True), StructField('driver_rating', DoubleType(), True), StructField('driver_phone', StringType(), True), StructField('driver_license', StringType(), True), StructField('vehicle_model', StringType(), True), StructField('vehicle_color', StringType(), True), StructField('license_plate', StringType(), True), StructField('pickup_address', StringType(), True), StructField('pickup_latitude', DoubleType(), True), StructField('pickup_longitude', DoubleType(), True), StructField('dropoff_address', StringType(), True), StructField('dropoff_latitude', DoubleType(), True), StructField('dropoff_longitude', DoubleType(), True), StructField('distance_miles', DoubleType(), True), StructField('duration_minutes', LongType(), True), StructField('booking_timestamp', TimestampType(), True), StructField('pickup_timestamp', StringType(), True), StructField('dropoff_timestamp', StringType(), True), StructField('base_fare', DoubleType(), True), StructField('distance_fare', DoubleType(), True), StructField('time_fare', DoubleType(), True), StructField('surge_multiplier', DoubleType(), True), StructField('subtotal', DoubleType(), True), StructField('tip_amount', DoubleType(), True), StructField('total_fare', DoubleType(), True), StructField('rating', DoubleType(), True)])

# Empty Streaming Table
dp.create_streaming_table('stg_rides')


# Bulk or Initial Load
@dp.append_flow(
    target = "stg_rides",
    # name = "<flow-name>", # optional, defaults to function name
    #once = True, # optional
    #spark_conf = {"<key>" : "<value", "<key" : "<value>"}, # optional
    #comment = "<comment>"
) # optional

def rides_bulk():
    df=spark.readStream.table('bulk_rides')
    df=df.withColumn('booking_timestamp', to_timestamp(col('booking_timestamp').cast('timestamp')))
    return df


# Streaming Load
@dp.append_flow(
    target = "stg_rides",
    # name = "<flow-name>", # optional, defaults to function name
    #once = True, # optional
    #spark_conf = {"<key>" : "<value", "<key" : "<value>"}, # optional
    #comment = "<comment>"
) # optional

def rides_stream():
    df=spark.readStream.table('rides_raw')
    df_parsed=df.withColumn('parsed_rides', from_json(col('rides'), rides_schema1)).select("parsed_rides.*")
    #df_parsed=df_parsed.withColumn('booking_timestamp', to_timestamp(col('booking_timestamp').cast('timestamp')))
    return df_parsed

