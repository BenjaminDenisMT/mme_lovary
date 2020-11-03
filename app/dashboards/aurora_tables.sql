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


WITH inventory_level AS (   
   SELECT
        inventory_level.inventory_id,
        FIRST_VALUE (inventory_level) OVER (PARTITION BY date_trunc('month', run_date), product_name, variants ORDER BY run_date) AS last_month_inventory,
        FIRST_VALUE (last_modification_time) OVER (PARTITION BY date_trunc('month', run_date), product_name, variants ORDER BY run_date) AS first_modification_time,
        FIRST_VALUE (run_date) OVER (PARTITION BY date_trunc('month', run_date), product_name, variants ORDER BY run_date) AS last_run_date,
        product_name,
        variants
    FROM 
        inventory_level
    LEFT JOIN
        products_informations
        ON inventory_level.inventory_id = products_informations.inventory_id
),
last_inventory_date AS (
    SELECT 
        inventory_id,
        date_trunc('month', last_run_date) AS month_,
        last_month_inventory,
        first_modification_time,
        last_run_date,
        product_name,
        variants
    FROM
        inventory_level
    GROUP BY
        inventory_id,
        date_trunc('month', last_run_date),
        last_month_inventory,
        first_modification_time,
        last_run_date,
        product_name,
        variants
),
sales_per_month AS (
    SELECT 
        date_trunc('month', created_at) AS Date_,
        title AS item_type,
        variant_title AS item_size,
        sum(quantity) AS vente
    FROM 
        daily_orders
    GROUP BY
        date_trunc('month', created_at),
        title,
        variant_title
),
rolling_average_per_3_months AS (
    SELECT 
        Date_,
        item_type AS item_type,
        item_size AS item_size,
        AVG(vente) OVER (PARTITION BY item_type, item_size ORDER BY Date_ ASC ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS average_3_month_sale
    FROM 
        sales_per_month
)
SELECT
    last_inventory_date.month_ - interval '1 month' AS Date_,
    last_inventory_date.product_name AS item_type,
    last_inventory_date.variants AS item_size,
    vente,
    last_month_inventory AS stock_apres_vente,
    average_3_month_sale AS average_3_month_sale
FROM 
    last_inventory_date
LEFT JOIN
    sales_per_month
    ON last_inventory_date.product_name = sales_per_month.item_type
    AND last_inventory_date.variants =  sales_per_month.item_size
    AND last_inventory_date.month_ - interval '1 month' = sales_per_month.Date_
LEFT JOIN
    rolling_average_per_3_months
    ON last_inventory_date.product_name = rolling_average_per_3_months.item_type
    AND last_inventory_date.variants =  rolling_average_per_3_months.item_size
    AND last_inventory_date.month_ - interval '1 month' = rolling_average_per_3_months.Date_