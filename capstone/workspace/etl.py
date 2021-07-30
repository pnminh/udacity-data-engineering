import configparser
import re

from pyspark.sql.functions import regexp_replace, udf, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Read config
config = configparser.ConfigParser()
config.read('capstone.cfg')
SAS_LABELS_DESCRIPTION_FILE_PATH = config['DATA']['SAS_LABELS_DESCRIPTION_FILE_PATH']
I94_DATA_FILE_PATH = config['DATA']['I94_DATA_FILE_PATH']
DEMOGRAPHICS_DATA_PATH = config['DATA']['SUPPLEMENTARY_DATASETS_DIR'] + 'us-cities-demographics.csv'
AIRPORT_CODE_DATA_PATH = config['DATA']['SUPPLEMENTARY_DATASETS_DIR'] + 'airport-codes_csv.csv'


def extract_countries(spark):
    """
    create a spark dataframe with country data
    :param spark: the spark session
    :return: the country dataframe
    """
    countries = get_label_codes("country")
    schema = StructType([
        StructField("country_code", StringType()),
        StructField("country_name", StringType())
    ])

    return spark.createDataFrame(
        data=countries,
        schema=schema
    )


def transform_countries_data(df_countries):
    """
    Clean countries data with setting all invalid country names to INVALID
    :param df_countries: the original df_countries
    :return: the cleaned df_countries
    """
    return df_countries \
        .withColumn('country_name',
                    regexp_replace('country_name', '^No Country.*|INVALID.*|Collapsed.*', 'INVALID')).dropDuplicates()


def extract_entry_ports(spark):
    """
    create a spark dataframe with port of entry data
    :param spark: the spark session
    :return:  the dataframe of ports of entry
    """
    entry_ports = get_label_codes("port_of_entry")
    schema = StructType([
        StructField("port_code", StringType()),
        StructField("port_name", StringType())
    ])
    return spark.createDataFrame(
        data=entry_ports,
        schema=schema
    )


def transform_entry_ports(df_entry_ports):
    """
    Clean entry port data with setting all invalid port names to INVALID
    :param df_entry_ports: the original df_entry_ports
    :return: the cleaned df_entry_ports
    """
    return df_entry_ports \
        .withColumn('port_name', regexp_replace('port_name', '^No PORT Code.*|Collapsed.*', 'INVALID')) \
        .withColumn("city_name",
                    udf(lambda port_name: port_name.split(',')[0].strip() if port_name != 'INVALID' else None)) \
        .withColumn('state_code',
                    udf(lambda port_name: port_name.split(',')[
                        1].strip() if port_name != 'INVALID' else None)).dropDuplicates()


def extract_travel_modes(spark):
    """
    create a spark dataframe with mode of travel data
    :param spark: the spark session
    :return: the dataframe of modes of travel
    """
    travel_modes = get_label_codes("mode_of_travel")
    schema = StructType([
        StructField("travel_mode_code", StringType()),
        StructField("travel_mode_name", StringType())
    ])
    return spark.createDataFrame(
        data=travel_modes,
        schema=schema
    )


def extract_us_states(spark):
    """
    create a spark dataframe with us state data
    :param spark: the spark session
    :return: the dataframe of us states
    """
    us_states = get_label_codes("us_state")
    schema = StructType([
        StructField("state_code", StringType()),
        StructField("state_name", StringType())
    ])
    return spark.createDataFrame(
        data=us_states,
        schema=schema
    )


def extract_visa_types(spark):
    """
    create a spark dataframe with visa type data
    :param spark: the spark session
    :return: the dataframe of us types
    """
    visa_types = get_label_codes("visa_type")
    schema = StructType([
        StructField("visa_type_code", StringType()),
        StructField("visa_type_name", StringType())
    ])
    return spark.createDataFrame(
        data=visa_types,
        schema=schema
    )


def get_label_codes(label_type):
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


def extract_airport_codes_data(spark):
    """
    Load airport codes data
    """
    return spark.read.csv(AIRPORT_CODE_DATA_PATH, header=True)


def transform_airport_codes_data(df_airports):
    """
    Clean and retrieve only necessary data
    :param df_airports:
    :return:
    """
    get_us_state_codes = udf(lambda iso_region: iso_region.split('-')[1])
    return df_airports.withColumn('state_code', get_us_state_codes(df_airports.iso_region)).select(
        col('ident').alias('id'), col('name').alias('airport_name'), 'state_code').where(
        'iso_country="US"').dropDuplicates()


def extract_immigration_data(spark):
    """
    load immigration data
    """
    return spark.read.format('com.github.saurfang.sas.spark').load(I94_DATA_FILE_PATH)


def transform_immigration_data(df_immigrations):
    """
    Clean and retrieve only necessary data
    :param df_immigrations:
    :return:
    """
    return df_immigrations.select(col('i94yr').alias('year_of_entry'), col('i94mon').alias('month_of_entry'),
                                  col('i94cit').alias('country_of_origin_code'),
                                  col('i94res').alias('country_of_residence_code'),
                                  col('i94mode').alias('mode_of_entry_code'),
                                  col('i94addr').alias('us_address_state_code'),
                                  col('airline'), col('visatype').alias('visa_type')).dropDuplicates()


def extract_us_cities_demographics_data(spark):
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


def transform_us_cities_demographics_data(df_us_dems):
    """
    Clean and retrieve only necessary data
    :param df_us_dems:
    :return:
    """
    return df_us_dems.select('city', 'state_code', 'total_population', 'foreign_born').dropDuplicates()
