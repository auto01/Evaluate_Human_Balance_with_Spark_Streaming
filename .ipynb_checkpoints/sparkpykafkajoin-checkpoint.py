from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, unbase64, base64, split, expr
from pyspark.sql.types import StructField, StructType, StringType, BooleanType, ArrayType, DateType, FloatType


kafkaRedisSchema = StructType (
    [
        StructField("key", StringType()),
        StructField("existType", StringType()),
        StructField("Ch", BooleanType()),
        StructField("Incr", BooleanType()),
        StructField("zSetEntries", ArrayType(
            StructType([
                StructField("element", StringType()),
                StructField("Score", StringType())
            ]))
        )
    ]
)

kafkaJSONSchema = StructType (
    [
        StructField("customer", StringType()),
        StructField("score", StringType()),
        StructField("email", StringType()),
        StructField("birthDay", StringType())
    ]
)


kafkaEventSchema = StructType(
    [
        StructField("customer", StringType()),
        StructField("score", FloatType()),
        StructField("riskDate", DateType())
    ]
)

spark = SparkSession.builder.appName("STEDI").getOrCreate()

spark.sparkContext.setLogLevel('WARN')


kafkaRedisDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","redis-server")\
    .option("startingOffsets","earliest")\
    .load() 

kafkaRedisDF = kafkaRedisDF.selectExpr("cast(value as string) value")

kafkaRedisDF.withColumn("value",from_json("value",kafkaRedisSchema)).select(col('value.*')).createOrReplaceTempView("RedisSortedSet")

kafkaRedisEncodedDF=spark.sql("select zSetEntries[0].element as Customer from RedisSortedSet")

kafkaRedisDecodedDF=kafkaRedisEncodedDF.withColumn('Customer',unbase64(kafkaRedisEncodedDF.Customer).cast("string"))

kafkaRedisDecodedDF.withColumn('Customer',from_json('Customer',kafkaJSONSchema)).select(col('Customer.*')).createOrReplaceTempView("CustomerRecords")

emailAndBirthDayStreamingDF=spark.sql("select email,split(birthDay,'-')[0] as birthYear from CustomerRecords where email is not null")



kafkaEventsDF = spark\
    .readStream\
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe","stedi-events")\
    .option("startingOffsets","earliest")\
    .load() 
                                   
kafkaEventsDF=kafkaEventsDF.selectExpr('cast(value as string)value')
kafkaEventsDF.withColumn('value',from_json('value',kafkaEventSchema)).select(col('value.*')).createOrReplaceTempView('CustomerRisk')

customerRiskStreamingDF=spark.sql("select customer,score from CustomerRisk")

stediDF=emailAndBirthDayStreamingDF.join(customerRiskStreamingDF,expr("""
customer=email"""))


#stediDF.writeStream.outputMode('append').format('console').start().awaitTermination()


stediDF.selectExpr(
  "cast(customer as string) as key",
  "to_json(struct(*)) as value")\
    .writeStream \
    .format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("topic", "stedi-score")\
    .option("checkpointLocation", "/tmp/kafkacheckpoint")\
    .start()\
    .awaitTermination()

# sink the joined dataframes to a new kafka topic to send the data to the STEDI graph application 
# +--------------------+-----+--------------------+---------+
# |            customer|score|               email|birthYear|
# +--------------------+-----+--------------------+---------+
# |Santosh.Phillips@...| -0.5|Santosh.Phillips@...|     1960|
# |Sean.Howard@test.com| -3.0|Sean.Howard@test.com|     1958|
# |Suresh.Clark@test...| -5.0|Suresh.Clark@test...|     1956|
# |  Lyn.Davis@test.com| -4.0|  Lyn.Davis@test.com|     1955|
# |Sarah.Lincoln@tes...| -2.0|Sarah.Lincoln@tes...|     1959|
# |Sarah.Clark@test.com| -4.0|Sarah.Clark@test.com|     1957|
# +--------------------+-----+--------------------+---------+
#
# In this JSON Format {"customer":"Santosh.Fibonnaci@test.com","score":"28.5","email":"Santosh.Fibonnaci@test.com","birthYear":"1963"}

