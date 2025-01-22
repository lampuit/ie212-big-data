from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, FloatType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")\
    .getOrCreate()

# Kafka configuration
kafka_broker = "127.0.0.1:9092"
kafka_topic = "Airfare_Prediction"

# Define the schema for incoming Kafka messages
schema = StructType([
    # StructField("Airline", StringType(), True),  # Corrected to StringType
    StructField("Journey_day", StringType(), True),
    StructField("Class", StringType(), True),
    StructField("Source", StringType(), True),
    StructField("Departure", StringType(), True),
    StructField("Total_stops", StringType(), True),
    StructField("Arrival", StringType(), True),
    StructField("Destination", StringType(), True),
    StructField("Duration_in_hours", FloatType(), True),
    StructField("Days_left", IntegerType(), True),
    StructField("Journey_month", IntegerType(), True),
    StructField("Weekend", IntegerType(), True),
    # StructField("Dep_min", IntegerType(), True),
    # StructField("Arrival_hour", IntegerType(), True),
    # StructField("Arrival_min", IntegerType(), True),
    # StructField("Price", DoubleType(), True),
])
# Load the trained model
model_path = "model"
try:
    model = PipelineModel.load(model_path)
    print("Model loaded successfully!")
except Exception as e:
    print(f"Failed to load model: {e}")

kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .load()



# Deserialize and parse the Kafka messages
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")  # Extract nested fields

# Handle null values in the parsed stream
parsed_stream = parsed_stream.na.drop()  # Remove rows with null values



# Apply the trained model to the streaming data
predictions = model.transform(parsed_stream)

# Rename and select relevant columns
predictions = predictions.withColumnRenamed("prediction", "Predict_Price") \
    .select(
        col("Source"),
        col("Destination"),
        col("Predict_Price")
    )

# Output predictions to the console in a tabular format
query = predictions.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .option("numRows", 3) \
    .trigger(processingTime="5 seconds") \
    .start()

query.awaitTermination()
