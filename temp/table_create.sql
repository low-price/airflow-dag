CREATE TABLE jheon735.coupang_product_price_history (
    datetime timestamp PRIMARY KEY,
    id INT8 REFERENCES jheon735.coupang_product_info (id),
    price int
);

CREATE TABLE jheon735.coupang_product_info (
    id INT IDENTITY(1,1) UNIQUE,
    item_id INT8 NOT NULL,
    vendor_item_id INT8 NOT NULL,
    url varchar(100),
    product_name varchar(100),
    image_url varchar(300)
);
