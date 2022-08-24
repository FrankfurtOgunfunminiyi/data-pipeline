from ast import Add, Import
from audioop import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro
#from pyspark.conf import SparkConf 
"""spark_conf = SparkConf()
        spark_conf.setAll([
        ('spark.master', 'spark://3.141.28.175:7077'), # <--- this host must be resolvable by the driver in this case pyspark (whatever it is located, same server or remote) in our case the IP of server
        ('spark.app.name', 'demo-lakehouse'),
        ('spark.submit.deployMode', 'client'),
        ('spark.ui.showConsoleProgress', 'true'),
        ('spark.eventLog.enabled', 'false'),
        ('spark.logConf', 'false'),
        ('spark.driver.bindAddress', '0.0.0.0'), # <--- this host is the IP where pyspark will bind the service running the driver (normally 0.0.0.0)
        ('spark.driver.host', '3.141.28.175'), # <--- this host is the resolvable IP for the host that is running the driver and it must be reachable by the master and master must be able to reach it (in our case the IP of the container where we are running pyspark
        ])"""
# Create sparksession 

spark = (SparkSession
        .builder
        .getOrCreate())

# create destination table 
spark.sql("CREATE TABLE TruncateTest1(ContractDate long,HE string,MW double, timestamp date) USING iceberg PARTITIONED BY (timestamp)")

def my_job():
        df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "3.141.28.175:9092") \
        .option("subscribe", "MP2-HOU-SQL-01.dbo.TruncateTest1") \
        .option("startingOffsets", "earliest") \
        .load()
        df.printSchema()
        df = df.selectExpr("CAST(value as STRING)","timestamp")
        df.printSchema()
        schema = (StructType()
                .add("ContractDate",LongType())
                .add("HE",StringType())
                .add("MW",DoubleType())  
                )
        finaldata = df.select(from_json(col("value"),schema).alias("TruncateTest1"),"timestamp")
        topic_data = finaldata.select("TruncateTest1.*","timestamp")
        topic_data.writeStream.foreachBatch(process_update).trigger(processingTime="5 seconds").format("iceberg").outputMode("append").option("path","s3://kafka-spark-iceberg-poc/sample_tables/TruncateTest1").option("checkpointLocation","s3://kafka-spark-iceberg-poc/checkpoint_loc/") # not created yet.start().awaitTermination()

     

def process_update(microbatch,batch_id):
    microbatch.createOrReplaceTempView("source")
    microbatch.cache()
    spark.sql("MERGE INTO TruncateTest1 t USING (SELECT * FROM source) ON t.timestamp = source.timestamp WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")
          
if __name__ == '__main__':
    my_job()
    
    
    
