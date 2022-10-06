import argparse
import findspark
findspark.init()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, rank, to_date, date_format
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, IntegerType, StructType, StringType, StructField, TimestampType, FloatType
import pandas as pd

def define_schema():
    sensor_schema = StructType([\
    StructField("sensor_id",IntegerType(),True),\
    StructField("sensor_description",StringType(),True),\
    StructField("sensor_name",StringType(),True),\
    StructField("installation_date",TimestampType(),True),\
    StructField("status",StringType(),True),\
    StructField("note",StringType(),True),\
    StructField("direction_1",StringType(),True),\
    StructField("direction_2",StringType(),True),\
    StructField("latitude",FloatType(),True),\
    StructField("longitude",FloatType(),True),\
    StructField("location",StringType(),True)\
    ])
    pedestrian_counting_schema = StructType([\
    StructField("ID",IntegerType(),True),\
    StructField("Date_Time",StringType(),True),\
    StructField("Year",IntegerType(),True),\
    StructField("Month",StringType(),True),\
    StructField("Mdate",IntegerType(),True),\
    StructField("Day",StringType(),True),\
    StructField("Time",IntegerType(),True),\
    StructField("Sensor_ID",IntegerType(),True),\
    StructField("Sensor_Name",StringType(),True),\
    StructField("Hourly_Counts",IntegerType(),True),\
    ])
    return sensor_schema, pedestrian_counting_schema

def extract_from_csv(schema, csvfile):
    '''Takes csv file's name and schema, returns the dataframe'''
    df = spark.read.option("header", True)\
                .schema(schema)\
                .csv(csvfile, header=True)
    return df

def transform(df):
    df = df.withColumn("Date_Time", to_date(col('Date_Time'), 'MMMM dd, yyyy hh:mm:ss a'))\
            .withColumn('Month', date_format(col('Date_Time'), 'M'))\
            .withColumn('Month', col('Month').cast('int'))\
            .withColumn('Hourly_Counts', col('Hourly_Counts').cast('int'))\
            .drop('Date_Time', 'Day', 'Time')
    return df

def get_top_by_day(df):
    window = Window.partitionBy(['Year','Month', 'Mdate'])\
                    .orderBy(col('sum(Hourly_Counts)').desc())
    transformed_df = df.groupBy(['Year','Month','Mdate','Sensor_ID']).sum('Hourly_Counts')\
                    .select('*', rank().over(window).alias('rank'))\
                    .filter(col('rank') <= 10)\
                    .withColumnRenamed('sum(Hourly_Counts)', 'Daily_Counts')\
                    .drop('rank')\
                    .orderBy(col('Year').asc(), col('Month').asc(),col('Mdate').asc(), col('Daily_Counts').desc())
    return transformed_df

def get_top_by_month(df):
    window = Window.partitionBy(['Year','Month'])\
                    .orderBy(col('sum(Hourly_Counts)').desc())
    transformed_df = df.groupBy(['Year','Month','Sensor_ID']).sum('Hourly_Counts')\
                    .select('*', rank().over(window).alias('rank'))\
                    .filter(col('rank') <= 10)\
                    .withColumnRenamed('sum(Hourly_Counts)', 'Monthly_Counts')\
                    .drop('rank')\
                    .orderBy(col('Year').asc(), col('Month').asc(), col('Monthly_Counts').desc())
    return transformed_df

def get_most_decline(df):
    most_decline_df=df[df['Year'].isin([2020,2021])]\
                            .groupBy(['Sensor_ID'])\
                            .pivot('Year')\
                            .sum('Hourly_Counts')\
                            .orderBy(col('Sensor_ID').asc())\
                            .withColumn('Coef', col('2020') - col('2021'))\
                            .orderBy(col('Coef').desc())\
                            .limit(1)
    return most_decline_df

def get_most_growth(df):
    most_growth_df=df[df['Year'].isin([2020,2021])]\
                            .groupBy(['Sensor_ID'])\
                            .pivot('Year')\
                            .sum('Hourly_Counts')\
                            .orderBy(col('Sensor_ID').asc())\
                            .withColumn('Coef', col('2021') - col('2020'))\
                            .orderBy(col('Coef').desc())\
                            .limit(1)
    return most_growth_df

def load_to_csv(output, *args):
    with pd.ExcelWriter(output,engine="xlsxwriter") as writer:
        for idx,df in enumerate(args):
            df.toPandas().to_excel(writer, sheet_name="Sheet"+f'{idx}', index=False)
    writer.save()
    print('Load process done')

if __name__ == "__main__":
    spark = SparkSession.builder.master('local')\
                            .appName('spark')\
                            .getOrCreate()
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_pedestrian_counting', '-ipc')
    parser.add_argument('--output', '-o')
    args = parser.parse_args()
    pedestrian_counting = args.input_pedestrian_counting
    output = args.output
    
    _, pedestrian_schema = define_schema()
    df = extract_from_csv(pedestrian_schema, pedestrian_counting)
    df_transformed = transform(df)
    top_by_day = get_top_by_day(df_transformed)
    top_by_month = get_top_by_month(df_transformed)
    most_decline = get_most_decline(df_transformed)
    most_growth = get_most_growth(df_transformed)

    load_to_csv(output, top_by_day, top_by_month, most_decline, most_growth)
