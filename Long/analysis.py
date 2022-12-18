from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import *

spark_application_name = "Spark_Application_Name"
spark = (SparkSession.builder.appName(spark_application_name).getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
parking = "/Users/longvo/EPITA/mlops/Long/output_data"
analysis_folder = "/Users/longvo/EPITA/mlops/Long/"


def read_disp_info(file, delimiter=',', header=False):
    devColumns = [StructField("date", TimestampType()),
                  StructField("id", IntegerType()), StructField("time", FloatType())]
    devSchema = StructType(devColumns)
    df = spark.read.options(delimiter=delimiter).csv(file, schema=devSchema, header=header)
    df.printSchema()
    print("Transactions :" + str(df.count()))
    return df


def write(df, path):
    df.write.mode('overwrite').option('header', True).csv(path)


def most_least_occupied_sensor_per_week(df):
    """
        At what day of the week there are most people parking and least people parking?
    """
    df = df.select(F.dayofweek(F.col("date")).alias("day"))
    df = df.groupBy("day").count().orderBy(F.col("count"))
    print("At what day of the week there are most people parking and least people parking?")
    print(f'The day where there are the most parking among the sensors is {df.tail(1)}')
    print(f'The day where there are the least parking among the sensors is  {df.head(1)}')
    write(df, f'{analysis_folder}analysis_data/question1_df')


def most_least_time_park(df):
    """
        What is the most and least time from a parking sensor?
    """
    df = df.select(F.to_date(F.col("date")).alias("day"), F.col("id"), F.col("time"))
    w_max = Window.partitionBy("id").orderBy(F.col("time").desc())
    df_max = df.withColumn("row", F.row_number().over(w_max)).filter(F.col("row") == 1).drop("row")
    w_min = Window.partitionBy("id").orderBy(F.col("time").asc())
    df_min = df.withColumn("row", F.row_number().over(w_min)).filter(F.col("row") == 1).drop("row")
    print('What is the most and least time from a parking sensor?')
    print(f'The most amount of time a car parked {df_max.head(1)}')
    print(f'The least amount of time a car parked {df_min.head(1)}')
    write(df_max, f'{analysis_folder}analysis_data/question2_df_max')
    write(df_min, f'{analysis_folder}analysis_data/question2_df_min')


def highest_lowest_time_parked_per_hour_per_sensor(df):
    """
        Throughout the week, what point of the day each parking sensor has most time parked and least time parked?
    """

    df = df.select(F.hour(F.col("date")).alias("hour"), F.col("id"), F.col("time"))
    df = df.groupBy("hour", "id").sum("time").orderBy(F.col("id"), F.col("sum(time)"))
    w_max = Window.partitionBy("id").orderBy(F.col("sum(time)").desc())
    df_max = df.withColumn("row", F.row_number().over(w_max)).filter(F.col("row") == 1).drop("row")
    w_min = Window.partitionBy("id").orderBy(F.col("sum(time)").asc())
    df_min = df.withColumn("row", F.row_number().over(w_min)).filter(F.col("row") == 1).drop("row")
    df_max.show(100)
    df_min.show(100)
    write(df_max, f'{analysis_folder}analysis_data/question3_df_max')
    write(df_min, f'{analysis_folder}analysis_data/question3_df_min')


if __name__ == '__main__':
    df = read_disp_info(parking, delimiter=',', header=True)
    most_least_occupied_sensor_per_week(df)
    most_least_time_park(df)
    highest_lowest_time_parked_per_hour_per_sensor(df)
