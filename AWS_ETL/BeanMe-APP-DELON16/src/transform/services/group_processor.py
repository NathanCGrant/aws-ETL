from typing import Dict, List, Tuple, Any

# Import custom utility and model
from models.data_models import ProductModel
from utils.file_utils import generate_csv

class GroupProcessor:
    """Processor for handling and transforming grouped transaction messages into
    structured CSVs.
    """ 

    def __init__(self, registry_service, s3_service, transform_service, logger):
        """
        Initializes the GroupProcessor with required services.

        Args:
            registry_service: Service for managing ID counters for transactions/baskets.
            s3_service: Service for interacting with S3 (storing data).
            transform_service: Service responsible for transforming raw messages into
                               structured data.
            logger: Logger instance for logging processing details.
        """
        self.registry_service = registry_service
        self.s3_service = s3_service
        self.transform_service = transform_service
        self.logger = logger

    def process_message_group(
        self,
        date: str,
        location_folder: str,
        group_messages: List[Dict[str, Any]],
        location_registry: Dict[str, int],
        product_registry: Dict[str, Any]
    ) -> Tuple[bool, bool]:
        """
        Processes a batch (group) of transaction messages for a specific date and location.

        Args:
            date: The date for the transactions (format: YYYY-MM-DD).
            location_folder: The name of the folder representing the location in S3.
            group_messages: A list of raw transaction messages to process.
            location_registry: A registry that keeps track of known locations and their IDs.
            product_registry: A registry that tracks known products and their IDs.

        Returns:
            Tuple:
                - location_updated (bool): True if any new locations were added.
                - products_updated (bool): True if any new products were added.
        """
        self.logger.info(f"📁 Processing group: {date} | 📍 Location: {location_folder}")

        # Determine how many transaction and basket IDs are needed
        total_transactions = len(group_messages)
        total_baskets = sum(
            len(ProductModel.transform_products(msg["products"]))
            for msg in group_messages
        )

        # Request a batch of unique IDs from the registry service
        id_counts = {
            "transaction": total_transactions,
            "basket": total_baskets
        }
        next_ids = self.registry_service.get_next_id_batch(id_counts)
        transaction_start_id = next_ids["transaction"]
        basket_start_id = next_ids["basket"]

        # Transform raw messages into structured data
        new_locations, transactions, baskets, products, location_updated, products_updated = (
            self.transform_service.transform_data(
                group_messages,
                location_registry,
                product_registry,
                transaction_start_id,
                basket_start_id,
            )
        )

        # If there are no valid transactions, skip saving to S3
        if not transactions:
            self.logger.warning(f"⚠️ No data to store for group: {date} | {location_folder}")
            return location_updated, products_updated

        # Convert transaction and basket data into CSV format
        transactions_csv = generate_csv(
            transactions,
            ["id", "date", "time", "location_id", "payment_type", "total_spend"]
        )
        baskets_csv = generate_csv(
            baskets,
            ["id", "transaction_id", "product_id"]
        )

        # Store CSV files to appropriate S3 folders
        self.s3_service.store_data(
            transactions_csv, f"transactions/{location_folder}/{date}/transactions.csv"
        )
        self.s3_service.store_data(
            baskets_csv, f"baskets/{location_folder}/{date}/baskets.csv"
        )

        self.logger.info(
            f"✅ Processed {len(transactions)} transactions for {date} | {location_folder}"
        )

        return location_updated, products_updated
