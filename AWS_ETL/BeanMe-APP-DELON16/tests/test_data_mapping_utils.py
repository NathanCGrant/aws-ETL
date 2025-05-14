import unittest
from src.load.utils.data_mapping_utils import determine_table_name

class TestDetermineTableName(unittest.TestCase):

    def test_metadata_file(self):
        self.assertIsNone(determine_table_name("metadata/id_counters.json"))

    def test_locations_file(self):
        self.assertEqual(determine_table_name("locations/locations.csv"), "locations")

    def test_products_file(self):
        self.assertEqual(determine_table_name("products/products.csv"), "products")

    def test_transactions_file(self):
        self.assertEqual(determine_table_name("transactions/2023/01/file.csv"), "transactions")

    def test_baskets_file(self):
        self.assertEqual(determine_table_name("baskets/daily/basket_2024.csv"), "baskets")

    def test_non_csv_file(self):
        self.assertIsNone(determine_table_name("transactions/2023/01/file.json"))

    def test_unknown_structure(self):
        with self.assertLogs("Data Mapping", level="WARNING") as cm:
            self.assertIsNone(determine_table_name("unknown/path/file.csv"))
            self.assertIn("Could not determine table name for path: unknown/path/file.csv", cm.output[0])

    def test_empty_string(self):
        with self.assertLogs("Data Mapping", level="WARNING") as cm:
            self.assertIsNone(determine_table_name(""))
            self.assertIn("Could not determine table name for path: ", cm.output[0])

if __name__ == '__main__':
    unittest.main()
