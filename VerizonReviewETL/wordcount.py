import pyspark
import time
from pyspark.sql import*
from pyspark import SparkContext, SparkConf
from datetime import datetime
from datetime import date

import pyspark.sql.functions as f
from pyspark.sql.functions import concat, concat_ws, lit, col, trim, length


conf = SparkConf().setMaster("local").setAppName("verizon-wordcount")
#start spark cluster
#if already started then get it else start it
sc = SparkContext.getOrCreate(conf=conf)
#initialize SQLContext from spark cluster
sqlContext = SQLContext(sc)

# reconstruct hourly file name
filename_suffix = str(datetime.today())[:13].replace(" ", "-")
infile = "scraped_" + filename_suffix + ".csv"

#
tableName = "reviewWordCount"
schemaName = "verizon"

try:
    file = sc.textFile("hdfs://0.0.0.0:19000/" + infile).cache()
    file.take(1)  # to test if file exists
    file_Available = "Yes"
except:
    file_Available = "No"
    print("File Not Available {}".format("hdfs://0.0.0.0:19000/" + infile))

if file_Available == "Yes":
    hdfsReviewsDF = sqlContext.read.csv("hdfs://0.0.0.0:19000/" + infile, header=True)

    ## filter word length <=3 which are meanless word like I, the , am, her, ...
    wordCountDF = hdfsReviewsDF.withColumn('Text', concat(col('Title'), lit(" "), col('ReviewText'))) \
        .withColumn('Keywords', f.explode(f.split(f.col('Text'), ' '))) \
        .groupBy('Device Model', 'Keywords') \
        .count() \
        .filter(length('Keywords') > 3) \
        .sort('count', ascending=False) \
        .withColumn('Date', lit(date.today().strftime("%m/%d/%y"))) \
        .take(250)

    ## load into Hive
    tblList = sqlContext.tableNames(schemaName)
    if tableName in tblList:
        print("Table exists")
        # insert code to insert/append
        wordCountDF.na.fill("").write.format('parquet').insertInto(schemaName + "." + tableName, overwrite=False)
    else:
        print("Table does not Exist")
        # insert code to create
        wordCountDF.na.fill("").write.mode('overwrite').format('parquet').saveAsTable(schemaName + "." + tableName)


else:
    sc.stop()