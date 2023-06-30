from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType
import textblob
from textblob import TextBlob

def sentiment(text):
    blob = TextBlob(text)
    sentiment = blob.sentiment.polarity
    if sentiment > 0:
        return "Positive"
    elif sentiment < 0:
        return "Negative"
    else:
        return "Neutral"

if __name__ == "__main__":   
    spark = SparkSession.builder.appName("stream_analyze").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    kafka_df = spark.readStream\
            .format("kafka")\
            .option("kafka.bootstrap.servers", "broker:9092")\
            .option("subscribe", "twitter")\
            .load()

    transformed_df = kafka_df.selectExpr("CAST(value AS STRING)").select(col("value").alias("tweet"))

    # user defined function
    sentiment_udf = udf(sentiment, StringType())
    transformed_df = transformed_df.withColumn("sentiment", sentiment_udf("tweet"))

    # comment this line below out if you want to see the non-grouped dataframe
    transformed_df = transformed_df.groupBy("sentiment").count()

    stream = transformed_df.writeStream\
            .outputMode("update")\
            .format("console")\
            .start()


    stream.awaitTermination()
