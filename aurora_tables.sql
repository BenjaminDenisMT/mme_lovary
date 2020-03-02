CREATE TABLE inventory_level (
    inventory_id VARCHAR(20) NOT NULL,
    inventory_level INTEGER NOT NULL,
    last_modification_time TIMESTAMPTZ NOT NULL,
    run_date DATE NOT NULL
);

CREATE TABLE products_informations (
    inventory_id VARCHAR(20) NOT NULL,
    product_name VARCHAR (100) NOT NULL,
    variants VARCHAR (50) NOT NULL
);

CREATE TABLE daily_orders (
    order_id VARCHAR(100) NOT NULL,
    variant_id VARCHAR(20) NOT NULL,
    title VARCHAR (100) NOT NULL,
    financial_status VARCHAR (100) NOT NULL,
    quantity INT NOT NULL,
    sku VARCHAR (100) NOT NULL,
    variant_title VARCHAR (100) NOT NULL,
    name VARCHAR (100) NOT NULL,
    price FLOAT NOT NULL,
    order_shipping_price FLOAT NOT NULL,
    total_discount FLOAT NOT NULL,
    order_total_tax FLOAT NOT NULL,
    province VARCHAR (50) NOT NULL,
    country VARCHAR (50) NOT NULL,
    created_at DATE NOT NULL,
    updated_at DATE NOT NULL,
    source_name VARCHAR (50) NOT NULL
);


SELECT
    inventory_level.inventory_id,
    inventory_level,
    last_modification_time,
    run_date,
    product_name,
    variants
FROM 
    inventory_level
LEFT JOIN
    products_informations
    ON inventory_level.inventory_id = products_informations.inventory_id;
