import unittest
from unittest.mock import patch, MagicMock, call

# Import the module to test
from src.create_tables.utils import (
    connect_to_redshift,
    create_main_tables,
    setup_redshift_schema
)

class TestRedshiftModule(unittest.TestCase):
    """Test cases for Redshift module functions."""

    def setUp(self):
        """Set up test fixtures before each test."""
        # Create a mock logger
        self.mock_logger = MagicMock()
        self.mock_logger.info = MagicMock()
        self.mock_logger.error = MagicMock()

        # Test configuration
        self.test_config = {
            "database-name": "test_db",
            "user": "test_user",
            "password": "test_password",
            "host": "test_host",
            "port": 5439
        }

    @patch('psycopg2.connect')
    def test_connect_to_redshift(self, mock_connect):
        """Test connect_to_redshift function with valid configuration."""
        # Set up mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Call the function
        conn, cursor = connect_to_redshift(self.test_config, self.mock_logger)

        # Check that psycopg2.connect was called with correct parameters
        mock_connect.assert_called_once_with(
            dbname="test_db",
            user="test_user",
            password="test_password",
            host="test_host",
            port=5439
        )

        # Check that cursor was created
        mock_conn.cursor.assert_called_once()

        # Check that info was logged
        self.mock_logger.info.assert_called_once_with("✅ Successfully connected to Redshift database.")

        # Check that the function returns the expected objects
        self.assertEqual(conn, mock_conn)
        self.assertEqual(cursor, mock_cursor)

    @patch('psycopg2.connect')
    def test_connect_to_redshift_default_port(self, mock_connect):
        """Test connect_to_redshift function with default port."""
        # Remove port from config to test default port
        config_without_port = self.test_config.copy()
        del config_without_port["port"]

        # Set up mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        # Call the function
        conn, cursor = connect_to_redshift(config_without_port, self.mock_logger)

        # Check that psycopg2.connect was called with default port
        mock_connect.assert_called_once_with(
            dbname="test_db",
            user="test_user",
            password="test_password",
            host="test_host",
            port=5439  # Default port
        )

    @patch('psycopg2.connect', side_effect=Exception("Connection error"))
    def test_connect_to_redshift_error(self, mock_connect):
        """Test connect_to_redshift function with connection error."""
        # Test that the exception is propagated
        with self.assertRaises(Exception) as context:
            connect_to_redshift(self.test_config, self.mock_logger)
        
        self.assertEqual(str(context.exception), "Connection error")

    def test_create_main_tables(self):
        """Test create_main_tables function."""
        # Create mock cursor
        mock_cursor = MagicMock()

        # Call the function
        create_main_tables(mock_cursor, self.mock_logger)

        # Check that cursor.execute was called 4 times (one for each table)
        self.assertEqual(mock_cursor.execute.call_count, 4)

        # Assert that logger.info was called for each table creation
        expected_log_calls = [
            call("Creating Locations table if not exists..."),
            call("Locations table created or already exists."),
            call("Creating Products table if not exists..."),
            call("Products table created or already exists."),
            call("Creating Transactions table if not exists..."),
            call("Transactions table created or already exists."),
            call("Creating Baskets table if not exists..."),
            call("Baskets table created or already exists.")
        ]
        self.mock_logger.info.assert_has_calls(expected_log_calls)

        # Check that the SQL statements for each table were executed
        # This verifies the first call (Locations table)
        self.assertIn("CREATE TABLE IF NOT EXISTS Locations", 
                      mock_cursor.execute.call_args_list[0][0][0])
        
        # Products table
        self.assertIn("CREATE TABLE IF NOT EXISTS Products", 
                      mock_cursor.execute.call_args_list[1][0][0])
        
        # Transactions table
        self.assertIn("CREATE TABLE IF NOT EXISTS Transactions", 
                      mock_cursor.execute.call_args_list[2][0][0])
        
        # Baskets table
        self.assertIn("CREATE TABLE IF NOT EXISTS Baskets", 
                      mock_cursor.execute.call_args_list[3][0][0])

    @patch('src.create_tables.utils.db_utils.connect_to_redshift')
    def test_setup_redshift_schema_success(self, mock_connect):
        """Test setup_redshift_schema function with successful execution."""
        # Set up mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = (mock_conn, mock_cursor)

        # Call the function
        setup_redshift_schema(self.test_config, self.mock_logger)

        # Check that connect_to_redshift was called with correct parameters
        mock_connect.assert_called_once_with(self.test_config, self.mock_logger)

        # Check that commit was called
        mock_conn.commit.assert_called_once()

        # Check that cursor and connection were closed
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

        # Check that success message was logged
        self.mock_logger.info.assert_any_call("✅ All SQL statements executed successfully.")

    @patch('src.create_tables.utils.db_utils.connect_to_redshift')
    @patch('src.create_tables.utils.db_utils.create_main_tables', side_effect=Exception("Table creation error"))
    def test_setup_redshift_schema_error(self, mock_create_tables, mock_connect):
        """Test setup_redshift_schema function with table creation error."""
        # Set up mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_connect.return_value = (mock_conn, mock_cursor)

        # Test that the exception is propagated
        with self.assertRaises(Exception) as context:
            setup_redshift_schema(self.test_config, self.mock_logger)
        
        self.assertEqual(str(context.exception), "Table creation error")

        # Check that error was logged
        self.mock_logger.error.assert_called_once_with("❌ Error during table creation: Table creation error")


if __name__ == '__main__':
    unittest.main()