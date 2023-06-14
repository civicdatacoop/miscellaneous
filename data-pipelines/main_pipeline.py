import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, lit, concat, date_trunc
import httpx

# === CONFIG SECTION ===
STORAGE_ACCOUNT: str = os.environ.get('STORAGE_ACCOUNT')
STORAGE_CONNECTOR: str = os.environ.get('STORAGE_CONNECTOR')
STORAGE_ACCESSOR: str = os.environ.get('STORAGE_ACCESSOR')
STORAGE_KEY: str = os.environ.get('STORAGE_KEY')
# a) Bronze Storage Container name:
BRONZE_CONTAINER: str = os.environ.get('BRONZE_CONTAINER')
# b) Silver Storage Container name:
SILVER_CONTAINER: str = os.environ.get('SILVER_CONTAINER')
# c) Gold Storage Container name
GOLD_CONTAINER: str = os.environ.get('GOLD_CONTAINER')
# d) Link to data catalogue
DATA_CATALOGUE_URL: str = os.environ.get('DATA_CATALOGUE_URL')
# e) Treatment for ClientID column:
CLIENT_ID_HASH_SUFFIX: str = os.environ.get('CLIENT_ID_HASH_SUFFIX')
HASH_SALT: str = os.environ.get('HASH_SALT')
# ~~~~~~~~~~~~~~~~~~~~~~

# === SET UP SPARK SESSION ===
session = SparkSession.builder.getOrCreate()
session.conf.set(STORAGE_ACCESSOR, STORAGE_KEY)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~

# === READ DATA CATALOGUE ===
data_catalogue_connector = httpx.get(DATA_CATALOGUE_URL, timeout=5)
data_catalogue: dict = data_catalogue_connector.json()
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~


# === FUNCTIONALITY BLOCK ===
def hash_client_id(data_frame, columns: list[str]):
    """Create salted hash of the ClientID column.
    Args:
        data_frame: Data frame with potential ClientID column
        columns (list[str]): List of columns with ClientID.
    Returns:
        Salted and hashed ClientID column inside a new Data Frame
    """
    hashed_client_id_df = data_frame
    for _column in columns:
        # Merge ClientID with HASH_SALT (salting procedure)
        hashed_client_id_df = hashed_client_id_df.withColumn(
            _column,
            concat(hashed_client_id_df[_column], lit(HASH_SALT))
        )
        # Compute hash value
        hashed_client_id_df = hashed_client_id_df.withColumn(
            "".join([_column, CLIENT_ID_HASH_SUFFIX]),
            sha2(
                hashed_client_id_df[_column].cast("Binary"), 256
            )
        )
        # Drop old columns
        hashed_client_id_df = hashed_client_id_df.drop(_column)

    return hashed_client_id_df


def round_datetime_columns(data_frame, columns: list[str], round_option: str):
    """Rounds datetime columns on required option.
    Args:
        data_frame: Input dataframe.
        columns (list[str]): List of columns which are rounded.
        round_option (str): Option for rounding
            (defined by PySpark, mainly "hour" and "month")
    Returns:
        New dataframe with rounded datetime columns.
    """
    datetime_rounded_df = data_frame
    for _column in columns:
        datetime_rounded_df = datetime_rounded_df.withColumn(
            _column,
            date_trunc(round_option, data_frame[_column])
        )
    return datetime_rounded_df
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~


# === MAIN APPLICATION LOOP ===
# Goes through all tables in the bronze data storage
for table_name, table in data_catalogue.items():
    # Get file name in bronze storage (TableName.txt - without dots)
    file_name = f'{table_name.replace(".", "")}.txt'
    # Read the file into Spark data frame
    raw_table_df = session.read.parquet(
        f"wasbs://{BRONZE_CONTAINER}@{STORAGE_CONNECTOR}/{file_name}"
    )
    # Hash ClientID column with salt
    hashed_client_id_table_df = hash_client_id(raw_table_df,
                                               table['client_id'])

    # Drop all identifiable columns
    deidentified_table_df = hashed_client_id_table_df.drop(
        *table['other_identifiable_columns']
    )

    # Truncate all times to hours
    hours_truncated_df = round_datetime_columns(deidentified_table_df,
                                                table['date_time'],
                                                "hour")

    # Truncate all date of births to months
    day_truncated_df = round_datetime_columns(hours_truncated_df,
                                              table['date_of_birth'],
                                              "month")

    # Check if there is anything to be written (skips empty tables)
    if len(
        set(table['columns_descriptions'].keys()) - (
            set(table['other_identifiable_columns'])
        )
    ) == 0:
        continue

    # Write table into silver storage
    day_truncated_df.write.mode('overwrite').parquet(
        f"wasbs://{SILVER_CONTAINER}@{STORAGE_CONNECTOR}/{file_name}"
    )

    # Drop columns with free text
    no_free_text_table_df = day_truncated_df.drop(
        *table['free_text_columns']
    )

    # Check if there is anything to be written (skips empty tables)
    if len(
        set(table['columns_descriptions'].keys()) - (
            set(table['free_text_columns']) |
            set(table['other_identifiable_columns'])
        )
    ) == 0:
        continue

    # Write table into gold storage
    no_free_text_table_df.write.mode('overwrite').parquet(
        f"wasbs://{GOLD_CONTAINER}@{STORAGE_CONNECTOR}/{file_name}"
    )
