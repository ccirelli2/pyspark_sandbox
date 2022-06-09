# Import Libraries
import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession


# Directories
DIR_ROOT = '/mnt/c/Users/ccirelli/OneDrive - American International Group, Inc/Desktop/GitHub/pyspark_sandbox'
DIR_DATA = os.path.join(DIR_ROOT, 'data')

# Globals
app_name = "my-first-spark-app"
DATA_FILENAME = 'test.csv'

# Initialize Spark
#conf = SparkConf().setAppName(app_name)
#sc = SparkContext(conf=conf)

# Initialize Spark Session
spark = SparkSession.builder.appName("test").getOrCreate()

# Load Data
df_spark = spark.read.csv(path=os.path.join(DIR_DATA, DATA_FILENAME))
print('################################')
print(df_spark.describe().show())
print('################################')

