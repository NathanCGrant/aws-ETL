import psycopg2
import logging
from typing import Dict, Tuple, Any

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Redshift Service")

def get_redshift_connection(config: Dict[str, Any]) -> Tuple[Any, Any]:
    """
    Establishes a connection to Redshift using the provided configuration.

    Args:
        config (dict): Redshift configuration containing database connection details.

    Returns:
        tuple: A tuple containing the connection object (`conn`) and cursor object (`cursor`),
               which can be used for interacting with Redshift.
    """
    # Connect to Redshift
    conn = psycopg2.connect(
        dbname=config["database-name"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config.get("port", 5439)
    )
    cursor = conn.cursor()
    return conn, cursor


def load_csv_to_redshift(
        s3_key: str,
        table_name: str,
        redshift_config: Dict[str, Any],
        bucket_name: str):
    """
    Loads a CSV file from S3 into a specified Redshift table.

    Args:
        s3_key (str): The S3 key (file path) of the CSV file to load into Redshift.
        table_name (str): The name of the Redshift table where data will be loaded.
        redshift_config (dict): Redshift configuration containing connection parameters.
        bucket_name (str): The name of the S3 bucket that contains the CSV file.

    Raises:
        RuntimeError: If there is an error during the connection or data loading process.
    """
    # Construct the full S3 path using the bucket name and S3 key
    s3_path = f"s3://{bucket_name}/{s3_key}"
    logger.info(f"📤 Preparing to load {s3_path} into Redshift table {table_name}")

    # Initialize connection and cursor variables
    conn = None
    cursor = None
    try:
        # Establish a connection to Redshift
        conn, cursor = get_redshift_connection(redshift_config)

        # If the table is 'locations' or 'products', truncate it before loading new data
        if table_name in ["locations", "products"]:
            logger.info(f"⚠️ Truncating table {table_name} before loading new data")
            
            # Remove all existing data from the table
            cursor.execute(f"TRUNCATE TABLE {table_name};")
            conn.commit()

        # Prepare the Redshift COPY command to load the CSV file from S3
        copy_command = f"""
            COPY {table_name}
            FROM '{s3_path}'
            IAM_ROLE 'arn:aws:iam::557690613516:role/RedshiftS3Role'
            FORMAT AS CSV
            IGNOREHEADER 1
            DATEFORMAT 'auto'
            TIMEFORMAT 'auto'
            ACCEPTINVCHARS;
        """

        logger.info(f"📝 Executing COPY command for {table_name}")
        cursor.execute(copy_command)  # Execute the COPY command
        conn.commit()  # Commit the changes to Redshift to load the data
        logger.info(f"✅ Successfully loaded {s3_key} into {table_name}")  # Log success

    except Exception as e:
        # If an error occurs, log the error and raise an exception
        logger.error(f"❌ Error loading {s3_key} into Redshift: {e}", exc_info=True)
        raise

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
