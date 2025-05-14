from typing import Dict, List, Tuple, Any

# Import custom models
from models.data_models import ProductModel, TransactionModel

class TransformService:
    """Service for transforming raw data into structured entities like locations,
    transactions, baskets, and products.
    """

    def __init__(self, registry_service, logger):
        """
        Initializes the transform service with required dependencies.

        Args:
            registry_service: Service responsible for managing product/location registries.
            logger: Logger instance for debug/info/error logging.
        """
        self.registry_service = registry_service
        self.logger = logger

    def transform_data(
        self,
        data: List[Dict[str, str]],
        location_registry: Dict[str, int],
        product_registry: Dict[str, Any],
        transaction_start_id: int,
        basket_start_id: int
    ) -> Tuple[
        List[Dict[str, Any]],  # New locations
        List[Dict[str, Any]],  # Transactions
        List[Dict[str, Any]],  # Baskets
        List[Dict[str, Any]],  # Products
        bool,                  # Location updated flag
        bool                   # Products updated flag
    ]:
        """
        Transforms a list of raw order records into structured output ready for
        storage or further processing.

        Args:
            data: List of raw order data dictionaries.
            location_registry: Registry of known locations with their IDs.
            product_registry: Registry of known products with details and next ID counter.
            transaction_start_id: Starting ID for assigning new transaction records.
            basket_start_id: Starting ID for assigning new basket records.

        Returns:
            A tuple containing:
                - New locations discovered during transformation
                - Transaction entries
                - Basket entries
                - Updated list of products
                - Whether locations were updated
                - Whether products were updated
        """
        new_locations = []
        transactions = []
        baskets = []

        transaction_id = transaction_start_id
        basket_id = basket_start_id
        location_updated = False

        # Capture whether the product registry was already marked as updated
        initial_products_updated = product_registry.get("updated", False)

        # Iterate through each order in the dataset
        for order in data:
            
            # Extract transaction metadata
            date, time = TransactionModel.transform_timestamp(order["transaction_timestamp"])
            payment_type = TransactionModel.get_payment_method(order["payment_method"])
            total_spend = TransactionModel.transform_transaction_total(order["transaction_total"])

            # Handle location logic (add new if not in registry)
            town = order["location_name"]
            if town not in location_registry:
                location_id = len(location_registry) + 1
                location_registry[town] = location_id
                new_locations.append({"id": location_id, "town": town})
                location_updated = True
            location_id = location_registry[town]

            # Create transaction record
            transactions.append({
                "id": transaction_id,
                "date": date,
                "time": time,
                "location_id": location_id,
                "payment_type": payment_type,
                "total_spend": total_spend
            })

            # Parse each product in the order and create basket entries
            product_list = ProductModel.transform_products(order["products"])
            for prod in product_list:
                
                # Assign or get existing product ID
                product_id = self.registry_service.get_product_id(product_registry, prod)

                # Create basket entry linking transaction and product
                baskets.append({
                    "id": basket_id,
                    "transaction_id": transaction_id,
                    "product_id": product_id
                })
                basket_id += 1

            # Increment for the next transaction
            transaction_id += 1

        # Determine if any new products were added during this transformation
        products_updated = product_registry.get("updated", False) != initial_products_updated

        return (
            new_locations,
            transactions,
            baskets,
            product_registry["products"],
            location_updated,
            products_updated
        )
