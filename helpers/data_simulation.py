import numpy
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import findspark

findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime


def do_simulate(mt_df, start_date, end_date):
    """
    For a given start and end date and the median temperatures,
    a simulated data is created for this provided period.
    This is done because global temperature dataset is only for 2013 and 
    would like to extend that as immigration data timeline is of 2016.
    """
    median_state_mapping = mt_df.to_numpy()

    days = int(
        (
            datetime.strptime(end_date, "%Y-%m-%d")
            - datetime.strptime(start_date, "%Y-%m-%d")
        ).days
    )
    simulate_mappings = {"date": [], "average_temp": [], "state": []}
    for state_mapping in median_state_mapping:
        for _ in range(days):
            if state_mapping[1] < 0:
                random_temp = numpy.random.uniform(low=state_mapping[1], high=10)
            else:
                random_temp = numpy.random.uniform(low=0, high=state_mapping[1])
            simulate_mappings["date"].append(start_date)
            simulate_mappings["average_temp"].append(random_temp)
            simulate_mappings["state"].append(state_mapping[0])
            temp_date = str(
                datetime.strptime(start_date, "%Y-%m-%d") + relativedelta(days=1)
            ).split(" ")[0]
            start_date = temp_date
            # print(start_date)
    simulate_df = pd.DataFrame.from_dict(simulate_mappings)
    simulate_df.to_csv("../data/simulate_temp.csv")


def simulate_temperatures(spark, start_date, end_date):
    """
    This function is a wrapper for performing a simulation transformation
    of the data required. So that the fact table can be joined to have temperature information.
    """
    global_temp_states = (
        spark.read.option("header", "true")
        .format("csv")
        .csv("../data/GlobalLandTemperaturesByState.csv", inferSchema=True)
    )
    global_temp_us = global_temp_states.filter(F.col("Country") == "United States")
    median_temperatures = global_temp_us.groupby("State").agg(
        F.percentile_approx("AverageTemperature", 0.5).alias("median_temperatures")
    )
    median_temperatures = median_temperatures.cache()
    mt_df = median_temperatures.toPandas()
    mt_df.head()
    mf_dt = {}
    for value in mt_df.iterrows():
        mf_dt[value[1][0]] = value[1][1]
    do_simulate(mt_df, start_date, end_date)


if __name__ == "__main__":
    # Timeline we are simulating the data.
    start_date = "2014-01-01"
    end_date = "2016-12-31"

    spark = SparkSession.builder.appName("Simulation Data Creation").getOrCreate()
    simulate_temperatures(spark, start_date, end_date)
