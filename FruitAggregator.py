from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

df1 = spark.readStream.format("kafka")\
  .option("kafka.bootstrap.servers", "localhost:9092")\
  .option("subscribe", "topicFruitBasket")\
  .option("failOnDataLoss","false")\
  .load()


print("*********************************************************************************")

print("***** PRINTING SCHEMA *****")
df1.printSchema()


dfAggr = df1.selectExpr("CAST(value AS STRING)").groupBy("value").count()

testQuery = dfAggr.writeStream\
  .format("console").outputMode("complete")\
  .start()

testQuery.awaitTermination()
