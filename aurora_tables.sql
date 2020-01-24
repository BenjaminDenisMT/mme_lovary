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


