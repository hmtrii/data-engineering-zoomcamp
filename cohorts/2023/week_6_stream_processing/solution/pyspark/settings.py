import pyspark.sql.types as T

INPUT_GREEN_PATH = '../resources/green_tripdata.csv'
INPUT_FHV_PATH = '../resources/fhv_tripdata.csv'

BOOTSTRAP_SERVERS = 'localhost:9092'

TOPIC_WINDOWED_RIDES_ALL = 'rides_all'

# PRODUCE_TOPIC_RIDES_CSV = CONSUME_TOPIC_RIDES_CSV = 'rides_csv'

FHV_KAFKA_TOPIC = 'rides_fhv'
GREEN_KAFKA_TOPIC = 'rides_green'

GREEN_SCHEMA = T.StructType([
    T.StructField("vendor_id", T.IntegerType()),
    T.StructField("pu_location_id", T.IntegerType()),
])

FHV_SCHEMA = T.StructType([
    T.StructField("dispatching_base_num", T.StringType()),
    T.StructField("pu_location_id", T.IntegerType()),
])
