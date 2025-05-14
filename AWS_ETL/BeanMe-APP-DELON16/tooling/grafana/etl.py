# etl.py
#
# ETL functions for local Grafana visualisation and SQL query development 
#
# Data Engineering Final Project 'BeanMeApp', April 2025, Avi Bercovich 
import typing
import os
import glob
import csv

from datetime import datetime

import mysql.connector

db_config = {
    'user'    :'root',
    'password':'GimmeData123',
    'host'    :'localhost',
    'database':'delon16'
}

data_dir = 'data'

def order_field_split(order:str) -> list:
#
# Splits order field into a list of products.
    return [product.strip() for product in order.split(",")]


def transform_timestamp(timestamp_str: str) -> tuple[str, str]:
#
# Returns a nicely formattded timestamp thingy
    try:

        dt = datetime.strptime(timestamp_str.strip(), "%d/%m/%Y %H:%M")

        return dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S")

    except ValueError as e:

        raise ValueError(f"Invalid timestamp format: {timestamp_str}") from e


def get_payment_method(payment_method: str) -> str:
#
# Returns a nicely formatted payment type

    # NOTE: Ideally this list of valid payment types should come out of 
    #        a db table someplace
    valid_payment_methods = ['card', 'cash', 'paypal', 'bags_of_salt']

    payment_method = payment_method_str.strip().lower()
    if payment_method in valid_payment_methods:

        return(payment_method.capitalize())

    else:

        raise ValueError(f"Invalid payment method: {payment_method}")


def get_product_size(product_data: str) -> str:
#
# Returns the product size

    # NOTE: Ideally this list of available_size should come out of 
    #        a db table someplace
    available_sizes = ['Regular', 'Large', 'Small']

    _size = available_sizes[0]

    for size in available_sizes: 

        if size in product_data:

            _size = size
            product_data = product_data.replace(size, "").strip()

            break

    return product_data, size

def get_product_flavour(product_data:str) -> tuple[str, str]:
#
# Returns the product flavour

    # NOTE: Ideally this list of flavours should come out of 
    #        a db table someplace
    flavours = ['Hazelnut', 
                'Vanilla', 
                'Caramel', 
                'Green', 
                'Peppermint', 
                'English breakfast', 
                'Berry Beautiful' ]

    _flavour = "Standard"               # NOTE, this can get dicey with other values  
                                        #       'Regular' and 'Standard' etc.
    for flavour in flavours:
        if flavour in product_data:    

            _flavour     = flavour
            product_data = product_data.replace(flavour, "").strip()

            break

    return product_data, _flavour

def insert_location(cursor, location:str) -> int:
#
# Insert a location into the 'locations' table.

    _id = 0

    query = "SELECT * FROM locations WHERE town = %s"
    cursor.execute(query, (location,))
    result = cursor.fetchone()

    if result is None:
    
        query = "INSERT INTO locations (town) VALUES (%s)"
        cursor.execute(query, (location, ))

        _id = cursor.lastrowid

    else:

        _id = result[0]

    return _id

def insert_product(cursor, product_spec:dict) -> int:
#
# Insert a product into the 'products' table.

    _id = 0

    query = "SELECT * FROM products WHERE name = %s AND flavour = %s AND size = %s "
    cursor.execute(query, (product_spec['name'],
                           product_spec['flavour'],
                           product_spec['size'],))
    result = cursor.fetchone()

    if result is None:

        query = "INSERT INTO products (name, size, flavour, price) VALUES (%s, %s, %s, %s)"
        cursor.execute(query, (product_spec['name'], 
                               product_spec['size'], 
                               product_spec['flavour'],
                               product_spec['price']))

        _id = cursor.lastrowid

    else:

        _id = result[0]

    return _id

def insert_transaction(cursor, transaction_data:list) -> int:
# 
# Inserts a transaction into the 'transactions' table

    date_time, location, customer, order, total_spend, payment_type, card_number, location_id = transaction_data

    # Convert the given date string to MySql/Mariadb timedate format
    dt = datetime.strptime(date_time, "%d/%m/%Y %H:%M")
    timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")

    query  = "INSERT INTO transactions (time, customer, total_spend, payment_type, card_number, location_id) "
    query += "VALUES (%s, %s, %s, %s, %s, %s)"
    cursor.execute(query, (timestamp, 
                           customer, 
                           total_spend, 
                           payment_type, 
                           card_number, 
                           location_id) )

    return cursor.lastrowid

def insert_basket(cursor, transaction_id:int, product_id:int):
#
# Insert a product into the basket table

    query  = "INSERT INTO baskets (transaction_id, product_id) "
    query += "VALUES (%s, %s)"

    cursor.execute(query, (transaction_id, product_id))


def process_csv(cursor, filename:str) -> int:
#
# Read a CSV file and insert its data into the database

    # setup primitive caching
    #
    # NOTE: Due to the excessive primitivenesshooddom of the caching mechanism 
    #       the keys in cache_products{} will contain spaces. 
    #       This is highly very utterly sub-optimal. Bad things will happen.
    cache_locations = {}                        # location:location_id
    cache_products  = {}                        # product:product_id

    with open(filename, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        next(csv_reader)                        # Skip the header row

        record_count = 0
        for record in csv_reader:

            # shoehorn record data into variables
            date_time, location, customer, order, total_spend, payment_type, card_number = record

            # pick-up location_id if in cache else register location in db
            if location in cache_locations:
                location_id = cache_locations[location]
            else:
                location_id = insert_location(cursor, location)
                cache_locations[location] = location_id

            # update row string with location_id
            record.append(location_id)

            # register the transaction
            transaction_id = insert_transaction(cursor, record)
            record_count  += 1

            # process order items relevant to this transaction
            products = order_field_split(order)
            for product_data in products:
                parts   = product_data.split('-')
                product = ' '.join(part.strip() for part in parts[:-1])
                price   = float(parts[-1].strip())

                _product, size    = get_product_size(product)
                _product, flavour = get_product_flavour(_product)
                name              = _product

                product_spec = {
                    'name'    : name,
                    'flavour' : flavour,
                    'size'    : size,
                    'price'   : price
                }

                # register product_spec in 'products' table or get product_id from product cache
                if product in cache_products:
                    product_id = cache_products[product]
                else:
                    product_id = insert_product(cursor, product_spec)
                    cache_products[product] = product_id

                # populate 'baskets' table with product_id for current transaction_id
                insert_basket(cursor, transaction_id, product_id)

        return record_count

def main(data_dir:str):
#
# Setup db connection and process CSV files

    # Connect to db and get a cursor
    db = mysql.connector.connect(**db_config)
    cursor = db.cursor()

    # Show we're connected to db
    cursor.execute("SELECT DATABASE();")
    result = cursor.fetchone()
    print(f"Connected to database: {result[0]}")

    # Process the CSV files in 'data'
    print(f"Processing CSV files from: {os.path.join(os.getcwd(), data_dir)}")

    files = glob.glob(os.path.join(os.getcwd(), data_dir, '*.csv'))
    for filename in files:
        print(f"Processing: {os.path.basename(filename)}", end=" ", flush=True)
        record_count = process_csv(cursor, filename)
        print(f"({record_count})")

        db.commit()         # Make it so, Number One

main(data_dir)
