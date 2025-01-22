from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import requests
from pyspark.sql.functions import from_json, col

# Create SparkSession
spark = SparkSession.builder \
    .appName("KafkaSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
    .getOrCreate()

# Load the trained model
model_path = "model"
try:
    model = PipelineModel.load(model_path)
    print("Model loaded successfully.")
except Exception as e:
    print(f"Error loading model: {e}")
    exit(1)

# Define schema
schema = StructType([
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
])

# Kafka configuration
kafka_broker = "localhost:9092"
kafka_topic = "Airfare_Prediction"

# Read data stream from Kafka
try:
    kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_broker) \
    .option("subscribe", kafka_topic) \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 10) \
    .load()

    print("Kafka stream initialized successfully.")
except Exception as e:
    print(f"Error initializing Kafka stream: {e}")
    exit(1)


# Parse data from Kafka
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Apply model for prediction
try:
    predictions = model.transform(parsed_stream)
    
    # Ensure no duplicate columns
    if "prediction" in predictions.columns:
        predictions = predictions.withColumnRenamed("prediction", "predicted_price")
    
    # Select only required columns (including predicted_price)
    predictions = predictions.select(
        "Journey_day",
        "Class",
        "Source",
        "Departure",
        "Total_stops",
        "Arrival",
        "Destination",
        "Duration_in_hours",
        "Days_left",
        "Journey_month",
        "Weekend",
        "predicted_price"
    )
    print("Model prediction applied successfully.")
except Exception as e:
    print(f"Error during model transformation: {e}")
    exit(1)

# Send prediction data to Flask API
def send_to_flask(data):
    try:
        response = requests.post("http://localhost:5000/predict", json=data)
        print(f"Response from Flask: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"Error sending data to Flask: {e}")

# Process batch data
def process_batch(batch_df, batch_id):
    if batch_df.isEmpty():
        print(f"Batch {batch_id} is empty. Skipping.")
        return
    
    try:
        results = batch_df.collect()
        print(f"Processing batch {batch_id}, {len(results)} rows.")
        for row in results:
            data = row.asDict()
            print("Data sent to Flask:", data)
            send_to_flask(data)
    except Exception as e:
        print(f"Error processing batch {batch_id}: {e}")

# Write prediction stream and send to Flask
try:
    query = predictions.writeStream \
        .foreachBatch(process_batch) \
        .outputMode("append") \
        .start()

    # Wait for the streaming to end
    query.awaitTermination()
except Exception as e:
    print(f"Error starting or running the streaming query: {e}")
    exit(1)
