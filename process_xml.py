# This example shows how to process an XML in CDSW using a third party library
# Spark on Yarn and resources are distributed on HDFS

from __future__ import print_function
from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("XML")\
    .getOrCreate()
sc = spark.sparkContext

# Read the schema.
df = spark.read.format('xml').options(rowTag='book').load('/samples/books.xml')
df.select("author", "_id").write \
    .format('xml') \
    .options(rowTag='book', rootTag='books') \
    .save('newbooks.xml')

spark.stop()

