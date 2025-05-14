import psycopg2

def connect_to_redshift(config, logger):
    """
    Establishes a connection to the Redshift database.

    Args:
        config (dict): A dictionary containing Redshift connection details.
        logger (Logger): Logger instance for logging messages.

    Returns:
        tuple: A tuple containing the connection and cursor objects.
    """
    # Connect to the Redshift cluster using provided credentials
    conn = psycopg2.connect(
        dbname=config["database-name"],
        user=config["user"],
        password=config["password"],
        host=config["host"],
        port=config.get("port", 5439)
    )
    cursor = conn.cursor()
    
    logger.info("✅ Successfully connected to Redshift database.")
    return conn, cursor


def create_main_tables(cursor, logger):
    """
    Creates the main tables required in the Redshift database.

    Args:
        cursor: Database cursor used to execute SQL commands.
        logger: Logger instance for outputting progress messages.
    """
    
    # Create Locations table
    logger.info("Creating Locations table if not exists...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Locations (
            id INT PRIMARY KEY,
            town VARCHAR(255) NOT NULL
        );
    """)
    logger.info("Locations table created or already exists.")
    
    # Create Products table
    logger.info("Creating Products table if not exists...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Products (
            id INT PRIMARY KEY,
            name VARCHAR(255) NOT NULL,
            flavour VARCHAR(255),
            size VARCHAR(255),
            price DECIMAL(10, 2) NOT NULL
        );
    """)
    logger.info("Products table created or already exists.")
    
    # Create Transactions table
    logger.info("Creating Transactions table if not exists...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Transactions (
            id INT PRIMARY KEY,
            date DATE NOT NULL,
            time TIME NOT NULL,
            location_id INT NOT NULL REFERENCES Locations(id),
            payment_type VARCHAR(255) NOT NULL,
            total_spend DECIMAL(10, 2) NOT NULL
        );
    """)
    logger.info("Transactions table created or already exists.")
    
    # Create Baskets table
    logger.info("Creating Baskets table if not exists...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Baskets (
            id INT PRIMARY KEY,
            transaction_id INT NOT NULL REFERENCES Transactions(id),
            product_id INT NOT NULL REFERENCES Products(id)
        );
    """)
    logger.info("Baskets table created or already exists.")


def setup_redshift_schema(redshift_config, logger):
    """
    Sets up the schema in Redshift by connecting to the database and creating
    required tables.

    Args:
        redshift_config (dict): Dictionary with connection parameters.
        logger: Logger instance for outputting process logs.
    """
    try:
        # Connect to Redshift and get connection and cursor
        conn, cursor = connect_to_redshift(redshift_config, logger)
        
        # Create required tables
        create_main_tables(cursor, logger)
        
        # Commit all SQL transactions
        conn.commit()
        logger.info("✅ All SQL statements executed successfully.")
        
        # Close cursor and connection
        cursor.close()
        conn.close()
    
    except Exception as e:
        logger.error(f"❌ Error during table creation: {e}")
        raise
