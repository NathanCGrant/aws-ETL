import json
from typing import Dict, List, Any

class RegistryService:
    """Service for managing and tracking registry data such as locations,
    products, and unique IDs.
    """

    def __init__(self, s3_service, config, logger):
        """
        Initialize the RegistryService.

        Args:
            s3_service: Service for interacting with S3 (read/write files).
            config: Configuration dictionary with file keys/paths.
            logger: Logger instance for logging messages.
        """
        self.s3_service = s3_service
        self.config = config
        self.logger = logger

    def get_next_id_batch(self, entity_types_count: Dict[str, int]) -> Dict[str, int]:
        """
        Reserves and returns the next batch of unique IDs for different entity types.

        Args:
            entity_types_count: Dict of entity types and how many IDs each needs.

        Returns:
            Dict mapping entity types to the first ID in their reserved batch.
        """
        try:
            counters = {}
            
            try:
                # Try to load existing ID counters from S3
                counters = self.s3_service.read_json(self.config["COUNTER_FILE_KEY"])
                self.logger.info(f"Retrieved existing ID counters: {counters}")
            
            except self.s3_service.s3.exceptions.NoSuchKey:
                # If the counter file doesn"t exist, start fresh
                self.logger.info("No existing ID counters found, initializing new counters")

            # Ensure all needed types are in the counter dict
            for entity_type in entity_types_count:
                if entity_type not in counters:
                    counters[entity_type] = 0

            result = {}
            for entity_type, count in entity_types_count.items():
                start_id = counters[entity_type] + 1
                counters[entity_type] += count
                result[entity_type] = start_id
                self.logger.info(
                    f"Reserved {count} IDs for {entity_type}: {start_id}"
                    f"to {counters[entity_type]}"
                )

            # Save updated counters back to S3
            self.s3_service.store_data(
                json.dumps(counters),
                self.config["COUNTER_FILE_KEY"]
            )
            self.logger.info(f"Updated ID counters saved to S3: {counters}")

            return result
        except Exception as e:
            self.logger.error(f"Error getting next IDs: {e}", exc_info=True)
            raise

    def read_all_locations(self) -> Dict[str, int]:
        """
        Reads the central locations registry CSV and builds a mapping of town names to IDs.

        Returns:
            Dict mapping location town names to their assigned IDs.
        """
        location_registry = {}

        try:
            rows = self.s3_service.read_csv(self.config["LOCATIONS_FILE_KEY"])
            for row in rows:
                location_registry[row["town"]] = int(row["id"])
            self.logger.info(f"Read {len(location_registry)} existing locations")
        except self.s3_service.s3.exceptions.NoSuchKey:
            self.logger.info("No existing locations file found")

        return location_registry

    def read_all_products(self) -> Dict[str, Any]:
        """
        Reads the central products registry CSV and builds an indexed product registry.

        Returns:
            A dictionary with:
                - keys: product signature keys to ID
                - products: list of product records
                - next_id: the next available product ID
                - updated: flag indicating if the registry changed
        """
        product_registry = {
            "keys": {},
            "products": [],
            "next_id": 1,
            "updated": False
        }

        try:
            rows = self.s3_service.read_csv(self.config["PRODUCTS_FILE_KEY"])
            for row in rows:
                product_id = int(row["id"])
                product = {
                    "id": product_id,
                    "name": row["name"],
                    "flavour": row["flavour"] if row["flavour"] != "None" else None,
                    "size": row["size"],
                    "price": float(row["price"])
                }
                product_registry["products"].append(product)

                # Construct a unique key for product comparison
                product_key = (
                    product["name"],
                    str(product["flavour"]),
                    product["size"],
                    f"{product["price"]:.2f}"
                )
                product_registry["keys"][product_key] = product_id

                # Update next_id tracker
                if product_id >= product_registry["next_id"]:
                    product_registry["next_id"] = product_id + 1

            self.logger.info(f"Read {len(product_registry["products"])} existing products")
        except self.s3_service.s3.exceptions.NoSuchKey:
            self.logger.info("No existing products file found")

        return product_registry

    def update_locations(self, location_registry: Dict[str, int]) -> None:
        """
        Updates the central locations CSV file in S3 with the current location registry.

        Args:
            location_registry: Dict of town names and their assigned IDs.
        """
        self.logger.info(
            f"Updating central locations file with {len(location_registry)} locations"
        )

        all_locations = [{"id": loc_id, "town": town} for town, loc_id in location_registry.items()]
        all_locations.sort(key=lambda x: x["id"])  # Sort by ID for consistency

        from utils.file_utils import generate_csv
        locations_csv = generate_csv(all_locations, ["id", "town"])
        self.s3_service.store_data(locations_csv, self.config["LOCATIONS_FILE_KEY"])

    def update_products(self, products: List[Dict[str, Any]]) -> None:
        """
        Updates the central products CSV file in S3 with the current list of products.

        Args:
            products: List of product records to store.
        """
        self.logger.info(f"Updating central products file with {len(products)} products")

        sorted_products = sorted(products, key=lambda x: x["id"])  # Ensure order consistency

        from utils.file_utils import generate_csv
        products_csv = generate_csv(sorted_products, ["id", "name", "flavour", "size", "price"])
        self.s3_service.store_data(products_csv, self.config["PRODUCTS_FILE_KEY"])

    def get_product_id(self, product_registry: Dict[str, Any], product: Dict[str, Any]) -> int:
        """
        Looks up or assigns a new product ID based on its characteristics.

        Args:
            product_registry: The existing registry containing products and keys.
            product: A dictionary representing a single product.

        Returns:
            An integer product ID, either existing or newly assigned.
        """
        price_str = f"{product["product_price"]:.2f}"

        product_key = (
            product["product_name"],
            str(product["product_flavour"]),
            product["product_size"],
            price_str
        )

        # Try to find an existing match for the product
        for existing_key, existing_id in product_registry["keys"].items():
            name, flavor, size, price = existing_key

            if (name == product["product_name"] and
                str(flavor) == str(product["product_flavour"]) and
                size == product["product_size"] and
                abs(float(price) - product["product_price"]) < 0.001):
                return existing_id  # Existing match found

        # If product doesn"t exist, assign new ID and store it
        product_id = product_registry["next_id"]
        product_registry["keys"][product_key] = product_id
        product_registry["products"].append({
            "id": product_id,
            "name": product["product_name"],
            "flavour": product["product_flavour"],
            "size": product["product_size"],
            "price": product["product_price"]
        })
        product_registry["next_id"] += 1
        product_registry["updated"] = True  # Mark registry as changed

        return product_id
