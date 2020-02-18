# This example shows how to process an XML in CDSW using a third party library
# Spark on Yarn and resources are distributed on HDFS

from __future__ import print_function
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .config('spark.jars.packages', 'com.databricks:spark-xml_2.11:0.8.0') \
    .config('spark.executor.memory', '16g') \
    .config('spark.executor.cores', '6') \
    .config('spark.cores.max', '8') \
    .config('spark.driver.memory','24g') \
    .appName("XML")\
    .getOrCreate()
sc = spark.sparkContext

# Read the schema.
df = spark.read.format('xml').options(rowTag='BILL').load('/tmp/xml/add.PER.20191100050000137260000000002_R.xml')
df.select("author", "_id").write \
    .format('xml') \
    .options(rowTag='BILL', rootTag='BILLS') \
    .save('/tmp/xml/new_bill.xml')

spark.stop()

