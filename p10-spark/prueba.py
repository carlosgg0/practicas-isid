from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import sys
import os

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

def main():

    spark_conf = SparkConf()
    spark_context = SparkContext(conf=spark_conf)

    # logger = spark_context._jvm.org.apache.log4j
    # logger.LogManager.getLogger("org").setLevel(logger.Level.WARN)

    data = [1, 2, 3, 4, 5]
    distributed_data = spark_context.parallelize(data)

    sum = distributed_data.reduce(lambda s1, s2: s1 + s2)

    print("The sum is ", str(sum))


if __name__ == "__main__":
    main()