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
OUTPUT_DATA_DIR = config['DATA']['OUTPUT_DATA_DIR']


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


def save_countries_dimension_table(df_countries):
    """
    save country dimension table as parquet files
    :param df_countries: the spark dataframe for countries
    """
    df_countries.write.parquet(OUTPUT_DATA_DIR + "dim_countries", mode='overwrite')


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
    get_city_name = udf(lambda port_name: port_name.split(',')[0].strip() if ',' in port_name else None)
    get_state_code = udf(lambda port_name: port_name.split(',')[1].strip() if ',' in port_name else None)
    return df_entry_ports \
        .withColumn("city_name", get_city_name(df_entry_ports.port_name)) \
        .withColumn('state_code', get_state_code(df_entry_ports.port_name)) \
        .withColumn('port_name', regexp_replace('port_name', '^No PORT Code.*|Collapsed.*', 'INVALID')).dropDuplicates()


def save_entry_ports_dimension_table(df_entry_ports):
    """
    save entry port dimension table as parquet files
    :param df_entry_ports: the spark dataframe for entry ports
    """
    df_entry_ports.write.partitionBy("state_code", "city_name").parquet(OUTPUT_DATA_DIR + "dim_entry_ports", mode='overwrite')


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


def save_travel_modes_dimension_table(df_travel_modes):
    """
    save travel mode dimension table as parquet files
    :param df_travel_modes: the spark dataframe for travel modes
    """
    df_travel_modes.write.parquet(OUTPUT_DATA_DIR + "dim_travel_modes", mode='overwrite')


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


def save_us_states_dimension_table(df_us_states):
    """
    save us state dimension table as parquet files
    :param df_us_states: the spark dataframe for us states
    """
    df_us_states.write.parquet(OUTPUT_DATA_DIR + "dim_us_states", mode='overwrite')


def extract_visa_modes(spark):
    """
    create a spark dataframe with visa mode data
    :param spark: the spark session
    :return: the dataframe of visa modes
    """
    visa_modes = get_label_codes("visa_mode")
    schema = StructType([
        StructField("visa_mode_code", StringType()),
        StructField("visa_mode_name", StringType())
    ])
    return spark.createDataFrame(
        data=visa_modes,
        schema=schema
    )


def save_visa_modes_dimension_table(df_visa_modes):
    """
    save visa mode dimension table as parquet files
    :param df_visa_modes: the spark dataframe for visa types
    """
    df_visa_modes.write.parquet(OUTPUT_DATA_DIR + "dim_visa_modes", mode='overwrite')


def extract_airport_data(spark):
    """
    Load airport codes data
    """
    return spark.read.csv(AIRPORT_CODE_DATA_PATH, header=True)


def transform_airport_data(df_airports):
    """
    Clean and retrieve only necessary data
    airport code either comes from iata code or local code
    :param df_airports:
    :return:
    """
    get_us_state_codes = udf(lambda iso_region: iso_region.split('-')[1])
    get_airport_code = udf(lambda iata_code, local_code: iata_code if iata_code is not None else local_code)
    return df_airports \
        .withColumn('state_code', get_us_state_codes(df_airports.iso_region)) \
        .withColumn('airport_code', get_airport_code(df_airports.iata_code, df_airports.local_code)) \
        .select(col('ident').alias('id'), col('name').alias('airport_name'), 'airport_code', 'state_code') \
        .where('iso_country="US" and airport_code is not null').dropDuplicates()


def save_airports_dimension_table(df_airports):
    """
    save airports dimension table as parquet files, partitioned by states
    the airports are grouped by states and airport codes
    :param df_airports: the spark dataframe for airports
    """
    df_airports.write.partitionBy("state_code", "airport_code") \
        .parquet(OUTPUT_DATA_DIR + "dim_airports", mode='overwrite')


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
    return df_immigrations.select(col('i94yr').alias('year_of_entry'),
                                  col('i94mon').alias('month_of_entry'),
                                  col('i94cit').alias('country_of_origin_code'),
                                  col('i94res').alias('country_of_residence_code'),
                                  col('i94addr').alias('us_address_state_code'),
                                  col('i94port').alias('port_of_entry_code'),
                                  col('i94mode').alias('travel_mode_code'),
                                  col('i94visa').alias('visa_mode_code'),
                                  col('airline'), col('visatype').alias('visa_type')).dropDuplicates()


def save_immigration_fact_table(spark, df_immigrations,
                                df_countries, df_us_states, df_entry_ports,
                                df_travel_modes, df_visa_modes):
    """
    Save immigration fact table based on immigration data and metadata from label dataset
    """
    df_immigrations.createOrReplaceTempView('immigration_temp_view')
    df_countries.createOrReplaceTempView('countries_temp_view')
    df_us_states.createOrReplaceTempView('us_state_temp_view')
    df_entry_ports.createOrReplaceTempView('ports_temp_view')
    df_travel_modes.createOrReplaceTempView('travel_modes_temp_view')
    df_visa_modes.createOrReplaceTempView('visa_modes_temp_view')

    df_immigrations_fact_table = spark.sql("""
                
                SELECT
                    itv.year_of_entry,itv.month_of_entry,ctv.country_code AS country_of_origin_code,
                    ctv2.country_code AS country_of_residence_code, ustv.state_code AS us_address_state_code,
                    ptv.port_code AS port_of_entry_code,tmtv.travel_mode_code,itv.visa_mode_code
                FROM immigration_temp_view itv
                    LEFT JOIN countries_temp_view ctv ON itv.country_of_origin_code=ctv.country_code
                    LEFT JOIN countries_temp_view ctv2 ON itv.country_of_residence_code=ctv2.country_code
                    LEFT JOIN us_state_temp_view ustv ON itv.us_address_state_code = ustv.state_code
                    LEFT JOIN ports_temp_view ptv ON itv.port_of_entry_code = ptv.port_code
                    LEFT JOIN travel_modes_temp_view tmtv ON itv.travel_mode_code = tmtv.travel_mode_code
                    LEFT JOIN visa_modes_temp_view vmtv ON itv.visa_mode_code = vmtv.visa_mode_code
                WHERE 
                    ctv.country_code IS NOT NULL AND
                    ctv2.country_code IS NOT NULL AND
                    ustv.state_code IS NOT NULL AND
                    ptv.port_code IS NOT NULL AND
                    tmtv.travel_mode_code IS NOT NULL AND
                    vmtv.visa_mode_code IS NOT NULL 
            """)
    df_immigrations_fact_table.write.partitionBy('country_of_residence_code',
                                                 'port_of_entry_code') \
        .parquet(OUTPUT_DATA_DIR + "fact_immigrations", mode='overwrite')


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


def save_us_cities_demographics_dimension_table(df_us_dems):
    """
    save us demographics dimension table as parquet files
    """
    df_us_dems.write.partitionBy("state_code", "city") \
        .parquet(OUTPUT_DATA_DIR + "dim_us_dems", mode='overwrite')


def validate_data_integrity(spark):
    """
    Make sure tables are not empty and the fact table have valid data
    """

    # load data from parquet files
    df_airports, df_countries, df_entry_ports, \
    df_immigrations, df_travel_modes, df_us_dems, \
    df_us_states, df_visa_modes \
        = get_data_from_parquet_files(spark)
    # Check tables for empty data
    error_message = ""
    if df_immigrations.count() == 0:
        error_message = "fact_immigration table does not have any data"

    if df_countries.count() == 0:
        error_message += "\ndim_countries table does not have any data"

    if df_entry_ports.count() == 0:
        error_message += "\ndim_entry_ports table does not have any data"

    if df_travel_modes.count() == 0:
        error_message += "\ndim_travel_modes table does not have any data"

    if df_us_states.count() == 0:
        error_message += "\ndim_us_states table does not have any data"

    if df_visa_modes.count() == 0:
        error_message += "\ndim_visa_modes table does not have any data"

    if df_airports.count() == 0:
        error_message += "\ndim_airports table does not have any data"

    if df_us_dems.count() == 0:
        error_message += "\ndim_us_dems table does not have any data"

    # Check fact_immigrations table to make sure it has valid data
    # check if df_immigrations has values that are not in dimension tables (leftanti: not contains)
    if df_immigrations.join(df_countries, df_immigrations.country_of_residence_code == df_countries.country_code,
                            'leftanti').count() != 0:
        error_message += "\ninvalid country_of_residence_code value"

    if df_immigrations.join(df_countries, df_immigrations.country_of_origin_code == df_countries.country_code,
                            'leftanti').count() != 0:
        error_message += "\ninvalid country_of_origin_code value(s) found"
    if df_immigrations.join(df_entry_ports, df_immigrations.port_of_entry_code == df_entry_ports.port_code,
                            'leftanti').count() != 0:
        error_message += "\ninvalid port_of_entry_code value(s) found"
    if df_immigrations.join(df_travel_modes, df_immigrations.travel_mode_code == df_travel_modes.travel_mode_code,
                            'leftanti').count() != 0:
        error_message += "\ninvalid travel_mode_code value(s) found"
    if df_immigrations.join(df_us_states, df_immigrations.us_address_state_code == df_us_states.state_code,
                            'leftanti').count() != 0:
        error_message += "\ninvalid df_us_states value(s) found"
    if df_immigrations.join(df_visa_modes, df_immigrations.visa_mode_code == df_visa_modes.visa_mode_code,
                            'leftanti').count() != 0:
        error_message += "\ninvalid visa_mode_code value(s) found"
    if error_message:
        raise Exception(error_message)


def get_data_from_parquet_files(spark):
    """
    Utility method to load all data from parquet files
    """
    df_immigrations = spark.read.parquet(OUTPUT_DATA_DIR + "fact_immigrations")
    df_countries = spark.read.parquet(OUTPUT_DATA_DIR + "dim_countries")
    df_entry_ports = spark.read.parquet(OUTPUT_DATA_DIR + "dim_entry_ports")
    df_travel_modes = spark.read.parquet(OUTPUT_DATA_DIR + "dim_travel_modes")
    df_us_states = spark.read.parquet(OUTPUT_DATA_DIR + "dim_us_states")
    df_visa_modes = spark.read.parquet(OUTPUT_DATA_DIR + "dim_visa_modes")
    df_airports = spark.read.parquet(OUTPUT_DATA_DIR + "dim_airports")
    df_us_dems = spark.read.parquet(OUTPUT_DATA_DIR + "dim_us_dems")
    return df_airports, df_countries, df_entry_ports, df_immigrations, \
           df_travel_modes, df_us_dems, df_us_states, df_visa_modes


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
        elif label_type == 'visa_mode':
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
