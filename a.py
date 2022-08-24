from ast import Add, Import
from audioop import add
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.avro.functions import from_avro
#from pyspark.conf import SparkConf 

def my_job():
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
        #
        spark = (SparkSession
        .builder
        .getOrCreate())
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
        
        ff = finaldata.select("TruncateTest1.*","timestamp")
        ff.printSchema()
        ff.writeStream.trigger(processingTime="2 seconds").format("console").outputMode("append").start().awaitTermination()
       
if __name__ == '__main__':
    my_job()
    
    
    
    # spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 a.py --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2 a.py
    # spark-submit --conf spark.dynamicAllocation.enabled=false --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 a.py
    
    # spark-submit --jars jar_files/spark-avro_2.12-3.1.3.jar --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 a.py --packages org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:0.13.2 a.py
   # Sample avro schema 
'''{ 
                        "namespace": "datamaking.avro",
                        "type": "record",
                        "name": "orders",
                        "fields": [
                                {"name": "order_id", "type": ["int", "null"]},
                                {"name": "order_product_name",  "type": ["string", "null"]},
                                {"name": "order_card_type", "type": ["string", "null"]},
                                {"name": "order_amount", "type": ["double", "null"]},
                                {"name": "order_datetime", "type": ["string", "null"]},
                                {"name": "order_country_name", "type": ["string", "null"]},
                                {"name": "order_city_name", "type": ["string", "null"]},
                                {"name": "order_ecommerce_website_name", "type": ["string", "null"]}
                        ]
                }

                '''