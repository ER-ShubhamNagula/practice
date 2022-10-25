import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,DoubleType

spark = SparkSession.builder.appName('swimming').getOrCreate()

idColumn = StructField("id",StringType(),True)
genderColumn = StructField("Gender",StringType(),True)
OccupationColumn = StructField("Occupation",StringType(),True)
swimTimeInSecondColumn = StructField("swimTimeInSecond",DoubleType(),True)

columnList = [idColumn,genderColumn,OccupationColumn,swimTimeInSecondColumn]
swimmerDfSchema = StructType(columnList)

#read data from csv
#swimmerDF = spark.read.format('CSV').option('InferSchema','true').option('header','true').load("H:\pyspark_examples\datasets\swamming.xlsx")
swimmerDF = spark.read.csv("H:\pyspark_examples\datasets\swamming.xlsx",header=True,schema=swimmerDfSchema)
#csv("H:\pyspark_examples\datasets\swamming.xlsx",header=True)
#swimmerDF.write.mode('append').csv(path='H:\pyspark_examples\datasets\output',header=True,sep=';')

#read data from json
corrData = spark.read.json(path="H:\pyspark_examples\datasets\json_data.json")
#creck schema of the dataframe
#corrData.printSchema()

#Save a DataFrame as a CSV File
#corrData.write.csv(path='H:\pyspark_examples\datasets\output',header=True,sep=',')

#Save a DataFrame as a JSON File
#swimmerDF.write.json(path='jsondata')
