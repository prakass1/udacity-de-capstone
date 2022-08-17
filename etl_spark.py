import pandas as pd
import findspark

findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, DoubleType, StringType
import datetime
from datetime import datetime as dtt
import configparser
import os
from helpers.udf_helpers import (
    format_ts,
    apply_impute_temp_func,
    apply_map_cities_func,
    days_of_stay,
    map_dfs,
)

config = configparser.ConfigParser()
config.read("config.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config.get("AWS", "AWS_ACCESS_KEY_ID")
os.environ["AWS_SECRET_ACCESS_KEY"] = config.get("AWS", "AWS_SECRET_ACCESS_KEY")
input_data = config.get("files", "input_data")
output_data = config.get("files", "output_data")


def get_mapping(spark, input_data):
    df = (
        spark.read.option("header", "true")
        .format("csv")
        .csv(input_data, inferSchema=True)
    )
    return df


def process_immdata(spark):
    """
    All the processing for the immigration dataset is done here. The steps performed are to obtain
    state information from the state_mapping data obtained via metadata of sas, applying udf functions
    for performing some transformations.
    """
    df_imm = spark.read.parquet(input_data + "/sas_data")
    df_states_mapping = get_mapping(spark, input_data + "/State_mapping.csv")
    # df_states_mapping=spark.read.option("header","true").format("csv").csv(input_data + "/State_mapping.csv",)
    df_imm.createOrReplaceTempView("i94_data")
    df_states_mapping.createOrReplaceTempView("states_mapping")
    out_frame = spark.sql(
        """
    SELECT * FROM
    i94_data as id LEFT JOIN states_mapping as sm ON id.i94ADDR = sm.code
    """
    )
    get_timestamp = udf(lambda x: format_ts(x))
    get_daysofstay = udf(days_of_stay, IntegerType())
    imm_data = out_frame.withColumn("arrdate_1", get_timestamp(out_frame.arrdate))
    imm_data = imm_data.withColumn("depdate_1", get_timestamp(imm_data.depdate))
    imm_data = imm_data.withColumn(
        "days_of_stay", get_daysofstay(imm_data.depdate_1, imm_data.arrdate_1)
    )
    imm_data = imm_data.filter(F.col("cicid").isNotNull())
    city_mappings = get_mapping(spark, input_data + "/city-mappings.csv")
    # city_mappings = spark.read.option("header","true").option("delimiter", ",").format("csv").csv(input_data + "/city-mappings.csv", inferSchema=True)
    city_mappings = city_mappings.cache()
    cm_df = city_mappings.toPandas()
    cm_dt = map_dfs(cm_df)
    b_cm_dt = spark.sparkContext.broadcast(cm_dt)
    imm_data = (
        imm_data.withColumn("arr_year", F.year("arrdate_1"))
        .withColumn("arr_month", F.month("arrdate_1"))
        .withColumn("arr_dayofmonth", F.dayofmonth("arrdate_1"))
        .withColumn("arr_weekofyear", F.weekofyear("arrdate_1"))
        .withColumn("arr_weekday", F.dayofweek("arrdate_1"))
    )
    # Adjusting city to keep format to get City based on City Code.
    imm_data = imm_data.withColumn(
        "adj_city", apply_map_cities_func(b_cm_dt)(F.col("i94port"))
    )
    return imm_data, df_states_mapping

def check_quality(df):
    if df.count() > 0:
        return "INFO: The table has records in it. The check is passed"
    raise ValueError("ERROR: The table has no records in it. The check is FAILED")

def process_us_demog(spark):
    """
    Processing for the us demographics dataset is done. Column name handling, city name formating is applied.
    """
    us_demog = (
        spark.read.option("header", "true")
        .option("delimiter", ";")
        .format("csv")
        .csv(input_data + "/us-cities-demographics.csv", inferSchema=True)
    )
    us_demog = us_demog.filter(F.col("State").isNotNull())
    us_demog = (
        us_demog.withColumnRenamed("Male Population", "male_pop")
        .withColumnRenamed("Female Population", "female_pop")
        .withColumnRenamed("Total Population", "total_pop")
        .withColumnRenamed("State Code", "state_code")
    )
    capitalize_city = udf(lambda x: x.lower().replace(" ", "").capitalize())
    us_demog = us_demog.withColumn("city_1", capitalize_city(us_demog.City))
    return us_demog


def process_global_temp(spark):
    """
    Processing for the global temperature dataset. This is a timeseries data and null are imputed by median,
    data spans only until 2013 to extend that a simulation is applied to generate synthethic data for the analysis.
    """
    global_temp_states = (
        spark.read.option("header", "true")
        .format("csv")
        .csv(input_data + "/GlobalLandTemperaturesByState.csv", inferSchema=True)
    )
    global_temp_us = global_temp_states.filter(F.col("Country") == "United States")
    median_temperatures = global_temp_us.groupby("State").agg(
        F.percentile_approx("AverageTemperature", 0.5).alias("median_temperatures")
    )
    median_temperatures = median_temperatures.cache()
    mt_df = median_temperatures.toPandas()
    mf_dt = map_dfs(mt_df)
    b_mf_dt = spark.sparkContext.broadcast(mf_dt)
    global_temp_us = global_temp_us.withColumn(
        "AverageTemperature_1",
        apply_impute_temp_func(b_mf_dt)(F.col("AverageTemperature"), F.col("State")),
    )
    # Drop Nulls from State column
    global_temp_us = global_temp_us.filter(F.col("State").isNotNull())
    # Extract YYYY MM DD Columns Separately.
    global_temp_us = (
        global_temp_us.withColumn("year", F.year("dt"))
        .withColumn("month", F.month("dt"))
        .withColumn("dayofmonth", F.dayofmonth("dt"))
        .withColumn("weekofyear", F.weekofyear("dt"))
        .withColumn("weekday", F.dayofweek("dt"))
    )
    return global_temp_us


def process_data(spark):
    try:
        """
        All the dataset are processed to create fact and dimension tables as follows:
        Dimension Tables:
        1. state_mapping - (Nested Dimension where demographics_us, airport_info are related)
        2. visa_info
        3. arrival_info
        4. global_temp_info

        Fact Table:
        imm_dynamics_us - Containing the required immigration data, arrival_info, temperature_info, state_code
        during the period.
        """
        imm_data, df_states_mapping = process_immdata(spark)
        global_temp_us = process_global_temp(spark)
        us_demog = process_us_demog(spark)
        imm_data.createOrReplaceTempView("imm_data")
        global_temp_us.createOrReplaceTempView("global_temp_us")
        us_demog.createOrReplaceTempView("us_demog")
        df_states_mapping.createOrReplaceTempView("states_mapping")

        # Get airport details.
        airport_details = (
            spark.read.option("header", "true")
            .option("delimiter", ",")
            .format("csv")
            .csv(input_data + "/airport-codes_csv.csv", inferSchema=True)
        )
        state_code_udf = udf(lambda x: x.split("-")[1])
        airport_details = airport_details.withColumn(
            "state_code", state_code_udf(airport_details.iso_region)
        )
        airport_details.createOrReplaceTempView("airport_details")
        joined_airport_details = spark.sql(
        """
        SELECT * FROM airport_details as fd LEFT JOIN states_mapping sm ON fd.state_code = sm.code
        """
        )
        # Quality check
        print(check_quality(joined_airport_details))
        joined_airport_details.createOrReplaceTempView("airport_info")

        # Get the arrival information from immigration data.
        capitalize_state = udf(lambda x: x.lower().capitalize() if x else None)
        arrival_info = spark.sql(
            """
            SELECT cicid, arrdate_1 as arrival_dt, depdate_1 as dep_dt, arr_year, arr_month, arr_dayofmonth, arr_weekofyear, arr_weekday, days_of_stay, state, i94addr as state_code, adj_city as city, gender, biryear as birthyear FROM imm_data"""
        )
        arrival_info = arrival_info.withColumn(
            "state_1", capitalize_state(arrival_info.state)
        )
        # Quality check
        print(check_quality(arrival_info))
        arrival_info.createOrReplaceTempView("arrival_info")
        arrival_info.write.partitionBy("arr_year", "arr_month").mode("overwrite").parquet(
            output_data + "/arrival_info.parquet"
        )

        # Get the visa information from immigration data.
        visa_info = spark.sql(
            """
            SELECT row_number() OVER (PARTITION BY '' ORDER BY '') as v_id, cicid as arrival_id, state,visatype,i94visa as visa_category
            FROM imm_data
            """
        )
        visa_info = visa_info.withColumn("state_1", capitalize_state(visa_info.state))
        # Quality check
        print(check_quality(visa_info))
        visa_info.createOrReplaceTempView("visa_info")
        visa_info.write.mode("overwrite").parquet(output_data + "/visa_info.parquet")

        # Get the airport information.
        airport_info = spark.sql(
        """
        SELECT ident as airport_ident, name as airport_name, iata_code, state, state_code  from  airport_info
        """
        )
        airport_info = airport_info.withColumn(
            "state", capitalize_state(airport_info.state)
        )
        # Quality check
        print(check_quality(airport_info))
        airport_info.write.partitionBy("state").mode("overwrite").parquet(
            output_data + "/airport_info.parquet"
        )

        # Global temperatures by date and state in USA.
        global_temperatures = spark.sql(
        """
        SELECT dt as date, AverageTemperature_1 as average_temp, state from global_temp_us
        """
        )
        # Simulated Data is created for global temperatures based on randomization to extend the range of global temperatures dataset to a given range in our case until 2017.
        simulate_data = (
            spark.read.option("header", "true")
            .option("delimiter", ",")
            .format("csv")
            .csv(input_data + "/simulate_temp.csv", inferSchema=True)
        )
        simulate_data = simulate_data.select(["date", "average_temp", "state"])
        global_temperatures = global_temperatures.union(simulate_data)
        global_temperatures = global_temperatures.withColumn(
            "state", capitalize_state(global_temperatures.state)
        )
        # Cache all computations.
        global_temperatures = global_temperatures.cache()
        # Quality check
        print(check_quality(global_temperatures))
        global_temperatures.createOrReplaceTempView("global_temperatures")
        global_temperatures.write.partitionBy("date", "state").mode("overwrite").parquet(
            output_data + "/global_temp_info.parquet"
        )

        # Demographics Data information by State and City in USA. Cannot join to fact directly since there is One-Many mapping with State table.
        demographics_us = spark.sql(
            """
        SELECT row_number() OVER (PARTITION BY '' ORDER BY '') as demog_id, city_1 as city, State as state, state_code, male_pop, female_pop, total_pop, Race as race
        FROM us_demog
        """
        )
        # Quality check
        print(check_quality(demographics_us))

        demographics_us.write.partitionBy("race").mode("overwrite").parquet(
            output_data + "/demographics_us.parquet"
        )

        # Facts Table combining information from dimensions: arrival, global_temperature, states_mapping (Nested mapping to airport_info, us_demographics by state_code)
        imm_dynamics_us = spark.sql(
            """
        SELECT row_number() OVER (PARTITION BY '' ORDER BY '') as imm_dyn_id, ai.cicid, ai.arrival_dt, ai.state_1 as state, vi.visa_category, gt.average_temp, sm.code as state_code
        FROM arrival_info ai JOIN global_temperatures gt ON ai.arrival_dt = gt.date AND ai.state_1 = gt.state JOIN visa_info vi ON vi.arrival_id = ai.cicid LEFT JOIN states_mapping sm ON sm.code = ai.state_code
        """
        )

        # Quality check
        print(check_quality(imm_dynamics_us))

        imm_dynamics_us.write.mode("overwrite").parquet(
            output_data + "/imm_dynamics_us.parquet"
        )
    except Exception as e:
        print(e)

if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Immigration Dynamics Transformation")
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1")
        .config("spark.jars.packages","saurfang:spark-sas7bdat:2.0.0-s_2.11")
        .getOrCreate()
    )
    process_data(spark)
