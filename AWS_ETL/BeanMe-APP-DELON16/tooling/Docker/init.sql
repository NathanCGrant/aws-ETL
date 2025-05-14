-- Create ENUM types (note: for PostgreSQL, not compatible with Redshift)
CREATE TYPE payment_type_enum AS ENUM ('Cash', 'Card');
CREATE TYPE product_size_enum AS ENUM ('Regular', 'Large');

CREATE TABLE Locations (
    id SERIAL PRIMARY KEY,
    town VARCHAR(255) NOT NULL
);

CREATE TABLE Products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    flavour VARCHAR(255),
    size product_size_enum NOT NULL,
    price DECIMAL(10, 2) NOT NULL
);

CREATE TABLE Transactions (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    time TIME NOT NULL,
    location_id INT NOT NULL,
    payment_type payment_type_enum NOT NULL,
    total_spend DECIMAL(10, 2) NOT NULL,
    FOREIGN KEY (location_id) REFERENCES Locations(id)
);

CREATE TABLE Baskets (
    id SERIAL PRIMARY KEY,
    transaction_id INT NOT NULL,
    product_id INT NOT NULL,
    FOREIGN KEY (transaction_id) REFERENCES Transactions(id),
    FOREIGN KEY (product_id) REFERENCES Products(id)
);