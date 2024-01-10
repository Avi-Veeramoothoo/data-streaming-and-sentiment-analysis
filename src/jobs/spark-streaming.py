import pyspark
from pyspark.sql import SparkSession

def start_streaming(spark):
    stream_df = (spark.readStream.format("socket")
                 .option("host", "localhost")
                 .option("port", 9999)
                 .load())
    
if __name__ == "__main__":
    spark_conn = SparkSession.builder.appName("SocketStreamConsumer").getOrCreate()
    
    start_streaming(spark_conn)