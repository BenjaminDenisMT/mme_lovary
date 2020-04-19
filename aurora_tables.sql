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
    source_name VARCHAR (50) NOT NULL,
    tags VARCHAR(75)
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

WITH  inventory_level AS (
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
        ON inventory_level.inventory_id = products_informations.inventory_id
)
SELECT 
    created_at AS Date_,
    title AS item_type,
    variant_title AS item_size,
    count(variant_id) AS vente,
    MAX(inventory_level) AS stock_apres_vente
FROM 
    daily_orders
LEFT JOIN
    inventory_level
    ON daily_orders.title = inventory_level.product_name
    AND daily_orders.variant_title = inventory_level.variants
    AND daily_orders.created_at = CAST(inventory_level.run_date - INTERVAL '1 DAY' AS DATE)
    
GROUP BY
    created_at,
    title,
    variant_title
order BY 
    created_at DESC 


