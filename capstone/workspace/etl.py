import configparser
import re

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Read config
config = configparser.ConfigParser()
config.read('capstone.cfg')
SAS_LABELS_DESCRIPTION_FILE_PATH = config['DATA']['SAS_LABELS_DESCRIPTION_FILE_PATH']
I94_DATA_FILE_PATH = config['DATA']['I94_DATA_FILE_PATH']
DEMOGRAPHICS_DATA_PATH = config['DATA']['SUPPLEMENTARY_DATASETS_DIR'] + 'us-cities-demographics.csv'
AIRPORT_CODE_DATA_PATH = config['DATA']['SUPPLEMENTARY_DATASETS_DIR'] + 'airport-codes_csv.csv'


def load_countries_df(spark):
    """
    create a spark dataframe with country data
    :param spark: the spark session
    :return: the country dataframe
    """
    countries = load_labels("country")
    schema = StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType())
    ])

    return spark.createDataFrame(
        data=countries,
        schema=schema
    )


def load_entry_ports_df(spark):
    """
    create a spark dataframe with port of entry data
    :param spark: the spark session
    :return:  the dataframe of ports of entry
    """
    entry_ports = load_labels("port_of_entry")
    schema = StructType([
        StructField("port_code", StringType()),
        StructField("port_name", StringType())
    ])
    return spark.createDataFrame(
        data=entry_ports,
        schema=schema
    )


def load_travel_modes_df(spark):
    """
    create a spark dataframe with mode of travel data
    :param spark: the spark session
    :return: the dataframe of modes of travel
    """
    travel_modes = load_labels("mode_of_travel")
    schema = StructType([
        StructField("travel_mode_code", StringType()),
        StructField("travel_mode_name", StringType())
    ])
    return spark.createDataFrame(
        data=travel_modes,
        schema=schema
    )


def load_us_states_df(spark):
    """
    create a spark dataframe with us state data
    :param spark: the spark session
    :return: the dataframe of us states
    """
    us_states = load_labels("us_state")
    schema = StructType([
        StructField("state_code", StringType()),
        StructField("state_name", StringType())
    ])
    return spark.createDataFrame(
        data=us_states,
        schema=schema
    )


def load_visa_types_df(spark):
    """
    create a spark dataframe with visa type data
    :param spark: the spark session
    :return: the dataframe of us types
    """
    visa_types = load_labels("visa_type")
    schema = StructType([
        StructField("visa_type_code", StringType()),
        StructField("visa_type_name", StringType())
    ])
    return spark.createDataFrame(
        data=visa_types,
        schema=schema
    )


def load_labels(label_type):
    """
    return a list of tuples of code and value for an I94 label
    so it can be used as a spark dataframe
    :param label_type: the label type
    :return: the list of tuples of code and value
    """
    codes = []
    with open('I94_SAS_Labels_Descriptions.SAS') as labels_desc_file:
        labels_desc = labels_desc_file.read()
        None
        if label_type == 'country':
            label_token_name = 'I94CIT & I94RES'
        elif label_type == 'port_of_entry':
            label_token_name = 'I94PORT'
        elif label_type == 'mode_of_travel':
            label_token_name = 'I94MODE'
        elif label_type == 'us_state':
            label_token_name = 'I94ADDR'
        elif label_type == 'visa_type':
            label_token_name = 'I94VISA'
        else:
            raise ValueError("Invalid value")
        label_data = labels_desc[labels_desc.index(label_token_name):]
        label_data = label_data[:label_data.index(';')]
        lines = label_data.split('\n')
        for line in lines:
            if re.match(r"^(\s+)(.*)=", line):
                parts = line.split("=")
                code = parts[0].strip().strip("'").strip()
                value = parts[1].strip().strip("'").strip()
                codes.append((code, value))
    return codes


# label_map = load_label_map()


def load_airport_codes_data(spark):
    """
    Load airport codes data
    """
    return spark.read.csv(AIRPORT_CODE_DATA_PATH, header=True)


def load_immigration_data(spark):
    """
    Load immigration data from SAS files
    """
    return spark.read.format('com.github.saurfang.sas.spark').load(I94_DATA_FILE_PATH)


def load_us_cities_demographics_data(spark):
    """Load US city demographics data
    """
    schema = StructType([
        StructField("city", StringType()),
        StructField("state", StringType()),
        StructField("median_age", DoubleType()),
        StructField("male_population", IntegerType()),
        StructField("female_population", IntegerType()),
        StructField("total_population", IntegerType()),
        StructField("number_of_veterans", IntegerType()),
        StructField("foreign_born", IntegerType()),
        StructField("average_household_size", DoubleType()),
        StructField("state_code", StringType()),
        StructField("race", StringType()),
        StructField("count", IntegerType())
    ])

    return spark.read.csv(DEMOGRAPHICS_DATA_PATH, sep=';', header=True, schema=schema)
