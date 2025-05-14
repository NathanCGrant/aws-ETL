import unittest
from unittest.mock import patch, MagicMock
from src.load.services.redshift_service import get_redshift_connection, load_csv_to_redshift

class TestRedshiftService(unittest.TestCase):

    @patch("src.load.services.redshift_service.psycopg2.connect")
    def test_get_redshift_connection_success(self, mock_connect):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn

        config = {
            "database-name": "mydb",
            "user": "admin",
            "password": "secret",
            "host": "redshift-cluster",
            "port": 5439
        }

        conn, cursor = get_redshift_connection(config)

        mock_connect.assert_called_once_with(
            dbname="mydb",
            user="admin",
            password="secret",
            host="redshift-cluster",
            port=5439
        )
        self.assertEqual(conn, mock_conn)
        self.assertEqual(cursor, mock_cursor)

    @patch("src.load.services.redshift_service.get_redshift_connection")
    def test_load_csv_to_redshift_success_with_truncate(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = (mock_conn, mock_cursor)

        load_csv_to_redshift(
            s3_key="data/locations.csv",
            table_name="locations",
            redshift_config={},
            bucket_name="my-bucket"
        )

        mock_cursor.execute.assert_any_call("TRUNCATE TABLE locations;")
        self.assertTrue(any("COPY locations" in call.args[0] for call in mock_cursor.execute.mock_calls))
        mock_conn.commit.assert_called()

    @patch("src.load.services.redshift_service.get_redshift_connection")
    def test_load_csv_to_redshift_success_without_truncate(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_get_conn.return_value = (mock_conn, mock_cursor)

        load_csv_to_redshift(
            s3_key="data/orders.csv",
            table_name="orders",
            redshift_config={},
            bucket_name="my-bucket"
        )

        mock_cursor.execute.assert_called_once()
        self.assertTrue("COPY orders" in mock_cursor.execute.call_args[0][0])
        mock_conn.commit.assert_called()

    @patch("src.load.services.redshift_service.get_redshift_connection")
    def test_load_csv_to_redshift_failure(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = Exception("COPY failed")
        mock_get_conn.return_value = (mock_conn, mock_cursor)

        with self.assertRaises(Exception):
            load_csv_to_redshift(
                s3_key="data/fail.csv",
                table_name="orders",
                redshift_config={},
                bucket_name="my-bucket"
            )

        mock_conn.close.assert_called()
        mock_cursor.close.assert_called()

if __name__ == "__main__":
    unittest.main()
