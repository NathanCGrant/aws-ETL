from datetime import datetime
from typing import Dict, List, Tuple, Any

class ProductModel:
    """Model representing a product with validation and transformation methods."""

    @staticmethod
    def extract_size_and_name(name_size_str: str) -> Tuple[str, str]:
        """
        Extracts the size and name from a string that combines both.

        Args:
            name_size_str: The product name with size embedded (e.g., "Large Coffee")

        Returns:
            A tuple containing:
                - size (e.g., "Large", "Regular")
                - cleaned product name (e.g., "Coffee")
        """
        size = "Regular"  # Default size
        name = name_size_str.strip()

        # Check for explicit size mentions
        if "Large" in name:
            size = "Large"
            name = name.replace("Large", "").strip()
        elif "Regular" in name:
            name = name.replace("Regular", "").strip()

        return size, name

    @staticmethod
    def get_product_size(size_str: str) -> str:
        """
        Converts the product size to a valid ENUM value.

        Args:
            size_str: Raw product size string

        Returns:
            Normalized product size: either "Regular" or "Large"

        Raises:
            ValueError if the input size is not recognized
        """
        size_str = size_str.strip().capitalize()
        if size_str in ["Regular", "Large"]:
            return size_str
        else:
            raise ValueError(f"Invalid product size: {size_str}")

    @staticmethod
    def transform_products(products_str: str) -> List[Dict[str, Any]]:
        """
        Transforms a raw string of products into a list of structured product
        dictionaries.

        Args:
            products_str: Product entries string (e.g., "Large Coffee - Vanilla - 3.5")

        Returns:
            A list of dictionaries with keys:
                - product_name
                - product_flavour
                - product_size
                - product_price

        Raises:
            ValueError if a product entry is incomplete or malformed
        """
        products = []

        # Split string into individual product entries
        for product in products_str.split(", "):
            parts = [p.strip() for p in product.split(" - ")]

            # Expect at least name-size and price, optionally flavour
            if len(parts) < 2:
                raise ValueError(f"Product information is incomplete: {product}")

            name_size = parts[0]
            price = float(parts[-1])
            flavour = parts[1] if len(parts) == 3 else "Standard"

            # Parse name and size
            size, name = ProductModel.extract_size_and_name(name_size)

            # Optional cleaning of name
            if flavour:
                name = name.replace("Flavoured", "").strip()

            # Build final product dictionary
            products.append({
                "product_name": name.title(),
                "product_flavour": flavour,
                "product_size": ProductModel.get_product_size(size),
                "product_price": price
            })

        return products


class TransactionModel:
    """Model for transaction data validation and transformation."""

    @staticmethod
    def transform_timestamp(timestamp_str: str) -> Tuple[str, str]:
        """
        Transforms a timestamp string into date and time strings compatible with
        Redshift schema.

        Args:
            timestamp_str: Raw timestamp string (e.g., "28/04/2025 14:30")

        Returns:
            A tuple of:
                - formatted date (YYYY-MM-DD)
                - formatted time (HH:MM:SS)

        Raises:
            ValueError if the timestamp is not in expected format
        """
        try:
            dt = datetime.strptime(timestamp_str.strip(), "%d/%m/%Y %H:%M")
            return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")
        except ValueError as e:
            raise ValueError(f"Invalid timestamp format: {timestamp_str}") from e

    @staticmethod
    def get_payment_method(payment_method_str: str) -> str:
        """
        Validates and normalizes the payment method string.

        Args:
            payment_method_str: Raw string indicating payment type

        Returns:
            "Cash" or "Card"

        Raises:
            ValueError for any unsupported payment method
        """
        payment_method_str = payment_method_str.strip().lower()
        if payment_method_str == "cash":
            return "Cash"
        elif payment_method_str == "card":
            return "Card"
        else:
            raise ValueError(f"Invalid payment method: {payment_method_str}")

    @staticmethod
    def transform_transaction_total(total_str: str) -> float:
        """
        Converts the total value string into a float.

        Args:
            total_str: Raw string of the total amount (e.g., "9.99")

        Returns:
            Float representation of the value

        Raises:
            ValueError if conversion fails
        """
        try:
            return float(total_str.strip())
        except ValueError as e:
            raise ValueError(f"Invalid total value: {total_str}") from e
