import configparser
from datetime import datetime, timedelta
from typing import List, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import udf, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType

# Read config
config = configparser.ConfigParser()
config.read('capstone.cfg')
SAS_LABELS_DESCRIPTION_FILE_PATH = config['DATA']['SAS_LABELS_DESCRIPTION_FILE_PATH']


def clean_states_data(states_df: DataFrame) -> DataFrame:
    """Clean states data."""
    return states_df \
        .filter('state_code != "99"')


def clean_countries_data(countries_df: DataFrame) -> DataFrame:
    """Clean I94 countries data by replacing invalid countries name with NA."""
    return countries_df \
        .withColumn('country_name', regexp_replace('country_name', '^No Country.*|INVALID.*|Collapsed.*', 'NA'))


def clean_ports_data(ports_df: DataFrame) -> DataFrame:
    """Clean ports data by splitting port name into city name and state code."""
    get_city_name = udf(lambda port_name: port_name.split(',')[0].strip() if port_name else None)
    get_state_code = udf(lambda port_name: port_name.split(',')[1].strip()
    if (port_name and len(port_name.split(',')) > 1) else None)

    return ports_df \
        .withColumn('city', get_city_name(ports_df.port_name)) \
        .withColumn('state_code', get_state_code(ports_df.port_name)) \
        .drop('port_name') \
        .dropna() \
        .dropDuplicates()


def clean_us_demographics_data(us_demographics_df: DataFrame) -> DataFrame:
    """Clean US demographics dataset."""
    return us_demographics_df \
        .dropDuplicates()


def clean_immigration_data(immigration_df: DataFrame) -> DataFrame:
    """
    Transform arrival date, departure date to regular dates and clean birth year column to ignore invalid values.
    """
    # Convert SAS dates to Python dates as it counts days from 1-1-1960
    get_isoformat_date = udf(lambda x: (datetime(1960, 1, 1).date() + timedelta(x)).isoformat() if x else None)
    get_valid_birth_year = udf(lambda yr: yr if (yr and 1900 <= yr <= 2016) else None)

    return immigration_df \
        .withColumn('arrdate', get_isoformat_date(immigration_df.arrdate)) \
        .withColumn('depdate', get_isoformat_date(immigration_df.depdate)) \
        .withColumn("biryear", get_valid_birth_year(immigration_df.biryear)) \
        .dropDuplicates()


def get_i94_visas(spark: SparkSession) -> DataFrame:
    """Maps visa category codes to category name as returns as a dataframe."""
    visa_category_code_to_name_pairs = get_data_from_sas_labels_file('I94VISA')
    schema = StructType([
        StructField("visa_category_id", StringType()),
        StructField("visa_category", StringType())
    ])

    return spark.createDataFrame(
        data=visa_category_code_to_name_pairs,
        schema=schema
    )


def get_i94_travel_modes(spark: SparkSession) -> DataFrame:
    """Maps travel mode ids to respective names and returns as a dataframe."""
    mode_code_to_name_pairs = get_data_from_sas_labels_file('I94MODE')
    schema = StructType([
        StructField("mode_id", StringType()),
        StructField("mode_name", StringType())
    ])

    return spark.createDataFrame(
        data=mode_code_to_name_pairs,
        schema=schema
    )


def get_i94_states(spark: SparkSession) -> DataFrame:
    """Maps state codes to state names and returns as a dataframe."""
    state_code_to_name_pairs = get_data_from_sas_labels_file('I94ADDR')
    schema = StructType([
        StructField("state_code", StringType()),
        StructField("state_name", StringType())
    ])

    return spark.createDataFrame(
        data=state_code_to_name_pairs,
        schema=schema
    )


def get_i94_ports(spark: SparkSession) -> DataFrame:
    """Translates port codes in SAS Labels Description file into a dataframe."""
    port_code_to_name_pairs = get_data_from_sas_labels_file('I94PORT')
    schema = StructType([
        StructField("port_code", StringType()),
        StructField("port_name", StringType())
    ])

    return spark.createDataFrame(
        data=port_code_to_name_pairs,
        schema=schema
    )


def get_i94_countries(spark: SparkSession) -> DataFrame:
    """Maps country code to country names and returns as a dataframe."""
    country_code_to_name_pairs = get_data_from_sas_labels_file('I94RES')
    schema = StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType())
    ])

    return spark.createDataFrame(
        data=country_code_to_name_pairs,
        schema=schema
    )


def get_data_from_sas_labels_file(label_name: str) -> List[Tuple[str, str]]:
    """
    Utility function to convert sas column label descriptions into tuples.
    :param label_name: Label of the column to transform
    :return: List of Tuple(code, value)
    """
    with open(SAS_LABELS_DESCRIPTION_FILE_PATH) as labels_file:
        file_data = labels_file.read()

    # Remove anything other than label data
    label_data = file_data[file_data.index(label_name):]
    label_data = label_data[:label_data.index(';')]

    lines = label_data.split('\n')
    code_value_pairs = list()
    for line in lines:
        parts = line.split('=')
        if len(parts) != 2:
            # Skip comment or other lines with no codes mapping
            continue
        code = parts[0].strip().strip("'")
        value = parts[1].strip().strip("'")
        code_value_pairs.append((code, value,))

    return code_value_pairs
