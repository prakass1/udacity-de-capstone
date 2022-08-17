import pyspark.sql.functions as F
import datetime
from datetime import datetime as dtt


def format_ts(value):
    """
    This is an udf function which reads the value which is unix epoch time and convert that into timestamp in the defined format.
    It returns a string as output.
    """
    epoch = dtt(1960, 1, 1)
    if value:
        ts = (epoch + datetime.timedelta(days=int(value))).strftime("%Y-%m-%d")
        return ts
    return None


def days_of_stay(d1, d2):
    """
    This is an udf function which returns number of days a person stayed in us.
    """
    if d1 and d2:
        return (dtt.strptime(d1, "%Y-%m-%d") - dtt.strptime(d2, "%Y-%m-%d")).days
    else:
        return -99999


def map_dfs(df):
    """
    This is an mapper function to convert df to k-v pairs
    """
    temp_dict = {}
    for value in df.iterrows():
        temp_dict[value[1][0]] = value[1][1]
    return temp_dict


# Broadcasting apply udf function, impute median average temp by state level.
def apply_impute_temp_func(mapping_broadcasted):
    """
    This is a broadcast udf which applies to broadcast impute median based on each state.
    """

    def f(temp, state):
        if temp:
            return temp
        return mapping_broadcasted.value.get(state)

    return F.udf(f)


# Broadcasting apply udf function, impute median average temp by state level.
def apply_map_cities_func(mapping_broadcasted):
    """
    This is a broadcast udf which apply to get city name for the code.
    """

    def f(c_code):
        if mapping_broadcasted.value.get(c_code):
            return mapping_broadcasted.value.get(c_code)
        return "UNKNW"

    return F.udf(f)
