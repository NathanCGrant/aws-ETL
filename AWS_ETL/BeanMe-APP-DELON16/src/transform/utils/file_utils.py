import csv
import io
from typing import List, Dict, Any

def generate_csv(data: List[Dict[str, Any]], headers: List[str]) -> str:
    """
    Generates a CSV string from a list of dictionaries.

    Args:
        data: The data to be converted to CSV format. Each dictionary represents a row.
        headers: The headers for the CSV file. These define the column names and order.

    Returns:
        The generated CSV data as a string.
        
    Raises:
        ValueError: If input data is invalid.
        RuntimeError: If CSV generation fails.
    """
    # Create an in-memory text stream to hold the CSV content
    output = io.StringIO()

    try:
        # Initialize the CSV writer with specified headers
        writer = csv.DictWriter(output, fieldnames=headers)

        # Write the header row to the CSV
        writer.writeheader()

        # Write all rows from the data list
        writer.writerows(data)

        # Retrieve the entire CSV content as a string
        return output.getvalue()
    
    except csv.Error as e:
        raise RuntimeError(f"CSV generation failed: {str(e)}")
    except Exception as e:
        raise ValueError(f"Invalid input data: {str(e)}")
    finally:
        # Ensure the StringIO object is closed
        output.close()