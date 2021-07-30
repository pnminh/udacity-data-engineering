import configparser

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Read config
config = configparser.ConfigParser()
config.read('capstone.cfg')
SAS_LABELS_DESCRIPTION_FILE_PATH = config['DATA']['SAS_LABELS_DESCRIPTION_FILE_PATH']
I94_DATA_FILE_PATH = config['DATA']['I94_DATA_FILE_PATH']
DEMOGRAPHICS_DATA_PATH = config['DATA']['SUPPLEMENTARY_DATASETS_DIR'] + 'us-cities-demographics.csv'
AIRPORT_CODE_DATA_PATH = config['DATA']['SUPPLEMENTARY_DATASETS_DIR'] + 'airport-codes_csv.csv'


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
