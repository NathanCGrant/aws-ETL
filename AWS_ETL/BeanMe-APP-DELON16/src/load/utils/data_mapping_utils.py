import logging
from typing import Optional

# Set up basic logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Data Mapping")

def determine_table_name(s3_key: str) -> Optional[str]:
    """
    Determines the Redshift table name based on the S3 key structure.

    Args:
        s3_key (str): The S3 key of the file (i.e., the file path in the S3 bucket).

    Returns:
        str: The name of the Redshift table, or None if it can"t be determined.
    """
    # Handle cases for specific central metadata files
    if s3_key == "metadata/id_counters.json":
        return None  # This file is not a CSV to load into Redshift, so return None
    elif s3_key == "locations/locations.csv":
        return "locations"  # Map the S3 key to the "locations" table in Redshift
    elif s3_key == "products/products.csv":
        return "products"  # Map the S3 key to the "products" table in Redshift

    # Handle regular data files organized by directory structure
    # Split the S3 key into parts using "/" as the delimiter
    path_parts = s3_key.split("/")

    # Check if the path has at least one part (e.g., "transactions", "baskets")
    if len(path_parts) >= 1:
        base_type = path_parts[0]  # Get the base folder name (first part of the path)
        
        # If the base type is "transactions" or "baskets" and the file is a CSV,
        # map it to the respective Redshift table
        if base_type in ["transactions", "baskets"] and s3_key.endswith(".csv"):
            return base_type  # Return the base type ("transactions" or "baskets")

    # If no valid table name can be determined, log a warning
    logger.warning(f"⚠️ Could not determine table name for path: {s3_key}")
    return None  # Return None if no matching structure was found
