from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import mean as _mean, stddev as _stddev, col
if __name__ =="__main__":
    conf = SparkConf().setAppName("Stock").setMaster("local[*]")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    """sc.textFile("/home/pawan_tilara/Documents/spark-assignment/stock_prices.csv").map(lambda line: line.split(",")) .filter(lambda line: len(line)>1) .map(lambda line: (line[0],line[1])).collect()
    stock = sc.textFile("/home/pawan_tilara/Documents/spark-assignment/stock_prices.csv")
    temp_value = stock.filter(lambda x:x.split(","))
    store_unique_date = {}
    print("type of temp_value :",type(temp_value))
    print("number of rows is :",temp_value.count())
    unique_date = temp_value.map(lambda x: (x[0], 1)).reduceByKey(lambda a,b: a+b)
    unique_date.take(5)
    for i in range(len(temp_value)):
        store_unique_date[temp_value[i][0]]+=1"""
    
    
    spark = SparkSession.builder.master("local[*]").appName("stock ").getOrCreate()
    df = spark.read.format("csv").option("header", "true").load("/home/pawan_tilara/Documents/spark-assignment/stock_prices.csv")
    df.createOrReplaceTempView("df")
    print(spark.sql("select date,open,close,volume from df").show())
    print(df.groupBy("date").count().show())

    # Compute the average daily return of every stock for every date. Print the results to screen
    print(spark.sql("select date , avg((close-open)*volume) as average_return from df group by date").show())
    answer = spark.sql("select date ,avg((close-open)*volume) as average_return from df group by date")
    answer.coalesce(1).write.csv("/home/pawan_tilara/Documents/spark-assignment/myresults1.csv")


    # Which stock was traded most frequently - as measured by closing price * volume - on average?
    print(spark.sql("select ticker,avg(open*volume) as frequency from df group by ticker order by frequency desc limit 1").show())


    
    

