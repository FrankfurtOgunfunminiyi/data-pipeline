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
        # create iceberg table in s3
        spark.sql("CREATE TABLE prod.db.sample (id bigint,data string,category string)USING iceberg PARTITIONED BY (category)")

if __name__ == '__main__':
    my_job()
        