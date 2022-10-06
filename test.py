from msilib import schema
from time import struct_time
import unittest
import findspark
findspark.init()
import pyspark
#from pedestrian_counting_spark import *
from pyspark.sql.functions import to_date, date_format, col
from pyspark.sql.types import *
from pyspark.sql import SparkSession

class SparkTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession
                     .builder
                     .master("local")
                     .appName("Unit-tests")
                     .getOrCreate())

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
    
    def extract_from_csv(self, schema, csvfile):
        '''Takes csv file's name and schema, returns the dataframe'''
        df = self.spark.read.option("header", True)\
                    .schema(schema)\
                    .csv(csvfile, header=True)
        return df

    def transform(self, df):
        df = df.withColumn("Date_Time", to_date(col('Date_Time'), 'MMMM dd, yyyy hh:mm:ss a'))\
            .withColumn('Month', date_format(col('Date_Time'), 'M'))\
            .withColumn('Month', col('Month').cast('int'))\
            .withColumn('Hourly_Counts', col('Hourly_Counts').cast('int'))\
            .drop('Date_Time', 'Day', 'Time')
        return df

    def test_extract_from_csv(self):
        schema = StructType([ \
                StructField("firstname",StringType(),True), \
                StructField("lastname",StringType(),True), \
                StructField("id", StringType(), True), \
                StructField("gender", StringType(), True), \
                StructField("salary", IntegerType(), True) \
            ])
        input_csv = './test_csv.csv'
        fake_data = [("James","Smith","1","M",3000),
                ("Michael","Rose","2","M",4000),
                ("Robert","Williams","3","M",4000),
                ("Maria","Jones","4","F",4000),
            ]
        #Expected dataframe
        expected_df = self.spark.createDataFrame(data=fake_data, schema=schema)

        #Input daatrame
        input_df = self.extract_from_csv(schema=schema, csvfile=input_csv)

        #Assert the output of the extract dataframe from csv to the expected dataframe
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, input_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        # Compare schema of input_df and expected_df
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in input_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(input_df.collect()))

    def test_transform(self):
        #Make input dataframe for transform function
        input_schema = StructType([\
                StructField("Date_Time",StringType(),True),\
                StructField("Year",IntegerType(),True),\
                StructField("Month",StringType(),True),\
                StructField("Mdate",IntegerType(),True),\
                StructField("Day",StringType(),True),\
                StructField("Time",IntegerType(),True),\
                StructField("Hourly_Counts",IntegerType(),True),\
        ])
        input_data = [("November 01, 2019 05:00:00 PM",2019,"November",1,"Friday",17,300),
                    ("November 25, 2018 09:00:00 PM",2018,"November",25,"Sunday",21,400),
                    ("May 10, 2022 08:00:00 PM",2022,"May",10,"Tuesday",20,455)]

        input_df = self.spark.createDataFrame(data=input_data, schema=input_schema)

        #Make expected output dataFrame
        expected_schema = StructType([\
                StructField("Year",IntegerType(),True),\
                StructField("Month",IntegerType(),True),\
                StructField("Mdate",IntegerType(),True),\
                StructField("Hourly_Counts",IntegerType(),True),\
                ])
        expected_data = [(2019,11,1,300),
                        (2018,11,25,400),
                        (2022,5,10,455)]
        expected_df = self.spark.createDataFrame(data=expected_data, schema=expected_schema)

        transformed_df = self.transform(input_df) 

        #Assert the output of the extract dataframe from csv to the expected dataframe
        field_list = lambda fields: (fields.name, fields.dataType, fields.nullable)
        fields1 = [*map(field_list, transformed_df.schema.fields)]
        fields2 = [*map(field_list, expected_df.schema.fields)]
        # Compare schema of input_df and expected_df
        res = set(fields1) == set(fields2)

        # assert
        self.assertTrue(res)
        # Compare data in input_df and expected_df
        self.assertEqual(sorted(expected_df.collect()), sorted(transformed_df.collect()))
