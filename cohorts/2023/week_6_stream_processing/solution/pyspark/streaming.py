from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from settings import GREEN_SCHEMA, FHV_SCHEMA, GREEN_KAFKA_TOPIC, \
    FHV_KAFKA_TOPIC, TOPIC_WINDOWED_RIDES_ALL


def read_from_kafka(consume_topic: str):
    # Spark Streaming DataFrame, connect to Kafka topic served at host in bootrap.servers option
    df_stream = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .option("subscribe", consume_topic) \
        .option("startingOffsets", "earliest") \
        .option("checkpointLocation", "checkpoint") \
        .load()
    return df_stream


def parse_ride_from_kafka_message(df, schema):
    """ take a Spark Streaming df and parse value col based on <schema>, return streaming df cols in schema """
    assert df.isStreaming is True, "DataFrame doesn't receive streaming data"

    df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    # split attributes to nested array in one Column
    col = F.split(df['value'], ', ')

    # expand col to multiple top-level columns
    for idx, field in enumerate(schema):
        df = df.withColumn(field.name, col.getItem(idx).cast(field.dataType))
    return df.select([field.name for field in schema])


def sink_console(df, output_mode: str = 'complete', processing_time: str = '5 seconds'):
    write_query = df.writeStream \
        .outputMode(output_mode) \
        .trigger(processingTime=processing_time) \
        .format("console") \
        .option("truncate", False) \
        .start()
    return write_query  # pyspark.sql.streaming.StreamingQuery


def sink_memory(df, query_name, query_template):
    query_df = df \
        .writeStream \
        .queryName(query_name) \
        .format("memory") \
        .start()
    query_str = query_template.format(table_name=query_name)
    query_results = spark.sql(query_str)
    return query_results, query_df


def sink_kafka(df, topic):
    write_query = df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092,broker:29092") \
        .outputMode('complete') \
        .option("topic", topic) \
        .option("checkpointLocation", "checkpoint") \
        .start()
    return write_query


def prepare_df_to_kafka_sink(df, value_columns, key_column=None):
    columns = df.columns

    df = df.withColumn("value", F.concat_ws(', ', *value_columns))
    if key_column:
        df = df.withColumnRenamed(key_column, "key")
        df = df.withColumn("key", df.key.cast('string'))
    return df.select(['key', 'value'])


def op_groupby(df, column_names):
    df_aggregation = df.groupBy(column_names).count()
    return df_aggregation


def op_windowed_groupby(df, window_duration, slide_duration):
    df_windowed_aggregation = df.groupBy(
        F.window(timeColumn=df.tpep_pickup_datetime, windowDuration=window_duration, slideDuration=slide_duration),
        df.vendor_id
    ).count()
    return df_windowed_aggregation


if __name__ == "__main__":
    spark = SparkSession.builder.appName('streaming-examples').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')

    # read_streaming data
    stream_green_df = read_from_kafka(consume_topic=GREEN_KAFKA_TOPIC)
    print(stream_green_df.printSchema())

    stream_fhv_df = read_from_kafka(consume_topic=FHV_KAFKA_TOPIC)
    print(stream_fhv_df.printSchema())

    # parse streaming data
    df_green = parse_ride_from_kafka_message(stream_green_df, GREEN_SCHEMA)
    print(df_green.printSchema())

    df_fhv = parse_ride_from_kafka_message(stream_fhv_df, FHV_SCHEMA)
    print(df_fhv.printSchema())

    # sink_console(df_rides, output_mode='append')

    df_rides_all = df_green.union(df_fhv, df_green.pu_location_id == df_fhv.pu_location_id)

    df_trip_count_by_pu_id = op_groupby(df_rides_all, ['pu_location_id'])

    # write the output out to the console for debugging / testing
    sink_console(df_trip_count_by_pu_id)
    # write the output to the kafka topic
    df_rides_all_messages = prepare_df_to_kafka_sink(df=df_rides_all,
                                                      value_columns=['count'], key_column='pu_location_id')
    kafka_sink_query = sink_kafka(df=df_rides_all_messages, topic=TOPIC_WINDOWED_RIDES_ALL)

    spark.streams.awaitAnyTermination()
