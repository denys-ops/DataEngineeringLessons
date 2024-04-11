SELECT client,
       price
FROM super_brand.sales.product_sales;

-- 1. group by
SELECT client,
       SUM(price)
FROM super_brand.sales.product_sales
GROUP BY client;

-- 2. group by vs window function
SELECT client,
       SUM(price) over (partition by client)
FROM super_brand.sales.product_sales;

-- 3. lag and lead

SELECT client,
       purchase_date,
       product,
       price,
       LAG(price) OVER (PARTITION BY client ORDER BY purchase_date)  AS previous_price,
       LEAD(price) OVER (PARTITION BY client ORDER BY purchase_date) AS next_price
FROM super_brand.sales.product_sales;

SELECT client,
       purchase_date,
       LAG(purchase_date) OVER (PARTITION BY client ORDER BY purchase_date)                 AS previous_purchase_date,
       purchase_date -
       LAG(purchase_date) OVER (PARTITION BY client ORDER BY purchase_date)                 AS days_from_previous_purchase
FROM super_brand.sales.product_sales;

-- 4. ROW_NUMBER
SELECT client,
       purchase_date,
       product,
       price,
       ROW_NUMBER() OVER (PARTITION BY client ORDER BY purchase_date DESC) AS row_num
FROM super_brand.sales.product_sales;

-- 5. ROW_NUMBER
SELECT client,
       purchase_date,
       product,
       price,
       RANK() OVER (PARTITION BY client ORDER BY purchase_date DESC)       AS price_rank,
       DENSE_RANK() OVER (PARTITION BY client ORDER BY purchase_date DESC) AS price_dense_rank
FROM super_brand.sales.product_sales;

-- 6. SUM, AVG
SELECT client,
       purchase_date,
       product,
       price,
       SUM(price) OVER (PARTITION BY client) AS total_spent_by_client,
       AVG(price) OVER (PARTITION BY client) AS average_spent_by_client,
       COUNT(*) OVER (PARTITION BY client)   AS number_of_purchases_by_client
FROM super_brand.sales.product_sales;

-- 7. Basic CTE Usage
WITH ProductSummary AS (SELECT product,
                               SUM(price) AS total_sales,
                               AVG(price) AS average_price
                        FROM super_brand.sales.product_sales
                        GROUP BY product)
SELECT product,
       total_sales,
       average_price
FROM ProductSummary
ORDER BY total_sales DESC;

-- 8. CTE for Date Filtering and Aggregation
WITH Q1Sales AS (SELECT client,
                        product,
                        price
                 FROM super_brand.sales.product_sales
                 WHERE purchase_date BETWEEN '2024-01-01' AND '2024-03-31')
SELECT client,
       SUM(price) AS total_spent,
       AVG(price) AS average_spent
FROM Q1Sales
GROUP BY client;

-- 9. Recursive CTE for Hierarchical Data

CREATE TABLE product_categories
(
    category_id        SERIAL PRIMARY KEY,
    category_name      VARCHAR(255),
    parent_category_id INTEGER,
    FOREIGN KEY (parent_category_id) REFERENCES product_categories (category_id)
);

INSERT INTO product_categories (category_id, category_name, parent_category_id)
VALUES (1, 'Electronics', NULL),
       (2, 'Computers', 1),
       (3, 'Laptops', 2),
       (4, 'Desktops', 2),
       (5, 'Accessories', 1),
       (6, 'Headphones', 5),
       (7, 'Mice', 5),
       (8, 'Cameras', NULL),
       (9, 'DSLR', 8),
       (10, 'Smartphones', 1);

select *
from product_categories;

WITH RECURSIVE CategoryHierarchy AS (SELECT category_id,
                                            category_name,
                                            parent_category_id,
                                            CAST(category_name AS VARCHAR(255)) AS path -- Cast to VARCHAR(255) in non-recursive term
                                     FROM product_categories
                                     WHERE parent_category_id IS NULL

                                     UNION ALL

                                     SELECT pc.category_id,
                                            pc.category_name,
                                            pc.parent_category_id,
                                            CAST(ch.path || ' > ' || pc.category_name AS VARCHAR(255)) AS path -- Ensure casting here as well
                                     FROM product_categories pc
                                              JOIN CategoryHierarchy ch ON pc.parent_category_id = ch.category_id)
SELECT category_id,
       category_name,
       parent_category_id,
       path
FROM CategoryHierarchy
ORDER BY path;

-- 10. Using CTEs for Data Cleaning
WITH ValidSales AS (SELECT client,
                           purchase_date,
                           product,
                           price
                    FROM super_brand.sales.product_sales
                    WHERE price > 0
                      AND client IS NOT NULL
                      AND product IS NOT NULL)
SELECT client,
       COUNT(*)   AS total_purchases,
       SUM(price) AS total_spent
FROM ValidSales
GROUP BY client;

-- 11. Subquery Example
SELECT client, product, price
FROM super_brand.sales.product_sales
WHERE price > (SELECT AVG(price)
               FROM super_brand.sales.product_sales);

-- 12. Correlated Subquery Example
SELECT client, product, price
FROM super_brand.sales.product_sales s1
WHERE price > (SELECT AVG(price)
               FROM super_brand.sales.product_sales s2
               WHERE s1.product = s2.product);

-- identifying the first purchase date for each client:

SELECT s1.client, s1.purchase_date
FROM super_brand.sales.product_sales s1
WHERE s1.purchase_date = (SELECT MIN(s2.purchase_date)
                          FROM super_brand.sales.product_sales s2
                          WHERE s1.client = s2.client);

-- 13. Basic Index Creation
CREATE INDEX idx_purchase_date ON super_brand.sales.product_sales (purchase_date);
-- B-tree is the default index type in PostgreSQL.
-- It's well-suited for general purposes, including equality and range queries.
DROP INDEX super_brand.sales.idx_purchase_date;

-- Hash indexes are optimized for equality comparisons using the = operator.
CREATE INDEX idx_purchase_date_hash ON super_brand.sales.product_sales USING hash (client);
DROP INDEX super_brand.sales.idx_purchase_date_hash;

-- GIN (Generalized Inverted Index) Indexes
-- GIN indexes are ideal for composite types like arrays or JSONB, where you might query elements within those types.
CREATE EXTENSION pg_trgm;
CREATE INDEX idx_purchase_date_gin ON super_brand.sales.product_sales USING gin (client);
DROP INDEX super_brand.sales.idx_purchase_date_gin;

-- GiST (Generalized Search Tree) Indexes
-- GiST indexes support various types of searches, including geometric and full-text search.
CREATE EXTENSION btree_gist;
DROP EXTENSION btree_gist;
CREATE INDEX idx_purchase_date_gist ON super_brand.sales.product_sales USING gist (client);
DROP INDEX super_brand.sales.idx_purchase_date_gist;

-- BRIN (Block Range Indexes)
-- BRIN indexes are suitable for very large tables with naturally ordered data.
CREATE INDEX idx_purchase_date_brin ON super_brand.sales.product_sales USING brin (client);
DROP INDEX super_brand.sales.idx_purchase_date_brin;

-- Partial Indexes
-- Partial indexes index a subset of rows and columns based on a specified condition.
-- This type of index is not specific to one indexing method; it can be applied with B-tree, GIN, GiST, etc.
CREATE INDEX idx_purchase_date_brin ON super_brand.sales.product_sales USING brin (client)
    WHERE purchase_date > '2024-01-01';
DROP INDEX super_brand.sales.idx_purchase_date_brin;

-- Unique Indexes
-- Unique indexes ensure that values in a column or a group of columns are unique across the table.
CREATE UNIQUE INDEX idx_purchase_date_uniq ON super_brand.sales.product_sales (price);
DROP INDEX super_brand.sales.idx_purchase_date_uniq;


-- 14. Composite Indexes
CREATE INDEX idx_client_purchase_date ON super_brand.sales.product_sales (client, purchase_date);
DROP INDEX super_brand.sales.idx_client_purchase_date;

-- 15. Using Indexes with Foreign Keys
CREATE INDEX idx_client_id ON super_brand.sales.product_sales (client_id);

-- 16. Example of Using EXPLAIN
EXPLAIN
SELECT *
FROM super_brand.sales.product_sales
WHERE purchase_date >= '2021-01-01';

-- 17. SELF JOIN
-- Example: Finding pairs of clients who bought the same product on sane date.
SELECT A.client AS client_a, B.client AS client_b, A.product,
       A.purchase_date || ' - ' || B.purchase_date as purchase_dates
FROM super_brand.sales.product_sales A
         JOIN super_brand.sales.product_sales B ON A.product = B.product
WHERE A.purchase_date = B.purchase_date
  AND A.client <> B.client;

-- 18. CROSS JOIN
-- Generating a list of all possible client-product combinations
SELECT clients.client, products.product
FROM (SELECT DISTINCT client FROM super_brand.sales.product_sales) clients
CROSS JOIN (SELECT DISTINCT product FROM super_brand.sales.product_sales) products;

-- 19. CROSS JOIN
-- A NATURAL JOIN automatically joins tables based on columns of the same name
CREATE TABLE super_brand.sales.client_info (
    client VARCHAR(50),
    email VARCHAR(100),
    PRIMARY KEY (client)
);

INSERT INTO super_brand.sales.client_info (client, email) VALUES
('Williams Karen', 'Williams_Karen@email.com'),
('Brown Linda', 'Brown_Linda@email.com');

select * from super_brand.sales.client_info;

SELECT *
FROM super_brand.sales.product_sales
NATURAL JOIN super_brand.sales.client_info;

-- 20. GROUP BY
SELECT client, SUM(price) AS total_sales
FROM super_brand.sales.product_sales
GROUP BY client;

-- 21. GROUP BY with Using ROLLUP

-- Calculate total sales by client, and then a grand total of sales across all clients.
SELECT client, SUM(price) AS total_sales
FROM super_brand.sales.product_sales
GROUP BY ROLLUP (client);

-- 22. GROUP BY with using CUBE

-- Calculate total sales for all combinations of client and product.
SELECT client, product, SUM(price) AS total_sales
FROM super_brand.sales.product_sales
GROUP BY CUBE (client, product);

-- 23. GROUP BY with using GROUPING SETS
-- GROUPING SETS allow for specifying multiple grouping sets in one query,
-- offering more flexibility than ROLLUP or CUBE.

-- Calculate total sales by client, by product, and a grand total, all in one query.

SELECT client, product, SUM(price) AS total_sales
FROM super_brand.sales.product_sales
GROUP BY GROUPING SETS ((client), (product), ());

-- 24. Filtering Groups with HAVING
SELECT client, SUM(price) AS total_sales
FROM super_brand.sales.product_sales
GROUP BY client
HAVING SUM(price) > 1000;

-- 25. Table Partitioning in PostgreSQL

CREATE TABLE super_brand.sales.product_sales_partitioning (
    sale_id SERIAL,
    client VARCHAR(50),
    purchase_date DATE,
    product VARCHAR(50),
    price DECIMAL,
    PRIMARY KEY (sale_id, purchase_date)  -- Including purchase_date in the primary key
) PARTITION BY RANGE (purchase_date);

-- OR
-- PARTITION BY LIST (product); -- For list partitioning

-- 26. Range Partitioning

-- Creating partitions by year
CREATE TABLE super_brand.sales.product_sales_2020 PARTITION OF super_brand.sales.product_sales_partitioning
FOR VALUES FROM ('2020-01-01') TO ('2021-01-01');

CREATE TABLE super_brand.sales.product_sales_2021 PARTITION OF super_brand.sales.product_sales_partitioning
FOR VALUES FROM ('2021-01-01') TO ('2022-01-01');

-- 27. List Partitioning

CREATE TABLE super_brand.sales.product_sales_partitioning_range (
    sale_id SERIAL,
    client VARCHAR(50),
    purchase_date DATE,
    product VARCHAR(50),
    price DECIMAL,
    PRIMARY KEY (sale_id, product)
) PARTITION BY LIST (product);

-- Creating partitions for specific product categories
CREATE TABLE super_brand.sales.product_sales_laptops PARTITION OF super_brand.sales.product_sales_partitioning_range
FOR VALUES IN ('laptop');

CREATE TABLE super_brand.sales.product_sales_tablets PARTITION OF super_brand.sales.product_sales_partitioning_range
FOR VALUES IN ('tablet');


SELECT * FROM super_brand.sales.product_sales_partitioning_range WHERE purchase_date >= '2020-01-01' AND purchase_date < '2021-01-01';

-- 28. Functions.
-- In PostgreSQL can be used to perform operations and return results.
CREATE OR REPLACE FUNCTION total_sales_for_client(client_name VARCHAR)
RETURNS DECIMAL AS $$
DECLARE
    total_sales DECIMAL := 0;
BEGIN
    SELECT SUM(price) INTO total_sales
    FROM super_brand.sales.product_sales
    WHERE client = client_name;

    RETURN total_sales;
END;
$$ LANGUAGE plpgsql;

select * from total_sales_for_client('Williams Sarah');

-- 29. Stored Procedure.
--  Can perform operations but do not directly return results like functions.
--  They can be used for complex business logic that involves multiple steps,
--  including data manipulation.
CREATE OR REPLACE PROCEDURE archive_old_sales(before_date DATE)
LANGUAGE plpgsql AS $$
BEGIN
    DELETE FROM super_brand.sales.product_sales
    WHERE purchase_date < before_date;
END;
$$;

CALL archive_old_sales('2020-01-01');

-- 30. Creating a Trigger
CREATE TABLE sales_audit (
    audit_id SERIAL PRIMARY KEY,
    log_description VARCHAR(255),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE OR REPLACE FUNCTION log_product_sales()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO sales_audit(log_description)
    VALUES ('A new sale has been recorded for ' || NEW.product || ' by client ' || NEW.client);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER sales_after_insert
AFTER INSERT ON super_brand.sales.product_sales
FOR EACH ROW EXECUTE FUNCTION log_product_sales();

SELECT * FROM sales_audit;

INSERT INTO super_brand.sales.product_sales (client, purchase_date, product, price)
VALUES ('Test Client', '2023-03-15', 'Test Product', 99.99);

-- 31. Transaction

BEGIN;

INSERT INTO super_brand.sales.product_sales (client, purchase_date, product, price) VALUES ('Client A', '2023-03-15', 'Product A', 150.00);
INSERT INTO super_brand.sales.product_sales (client, purchase_date, product, price) VALUES ('Client B', '2023-03-16', 'Product B', 250.00);

-- If both insertions were successful, commit the transaction.
COMMIT;

-- If there was an error, you might rollback the transaction.
ROLLBACK;

select * from super_brand.sales.product_sales;

-- 31. Transaction rollback

BEGIN;

-- First sales record: Valid
INSERT INTO super_brand.sales.product_sales (client, purchase_date, product, price)
VALUES ('Client X', '2023-10-01', 'Laptop', 1200);

-- Second sales record: Invalid due to a negative price
INSERT INTO super_brand.sales.product_sales (client, purchase_date, product, price)
VALUES ('Client Y', '2023-10-02', 'Tablet', -300);

-- Upon detecting the mistake, decide to rollback
ROLLBACK;

select * from super_brand.sales.product_sales;


-- 32. Docs
CREATE TABLE super_brand.sales.product_sales
/*
    Table: product_sales
    Schema: sales
    Database: super_brand

    Description:
    This table records the sales transactions for products sold by the super_brand company.
    Each record represents a single purchase transaction by a client.

    Columns:
    - client: VARCHAR(50)
      The name of the client who made the purchase. The maximum length of the name is 50 characters.

    - purchase_date: DATE
      The date on which the purchase was made.

    - product: VARCHAR(50)
      The name of the product that was purchased. The maximum length of the product name is 50 characters.

    - price: INT
      The price at which the product was sold, represented as an integer. This is the total sale price for the product.

    Notes:
    - The table does not enforce constraints like NOT NULL or UNIQUE on any columns.
    - The price is assumed to be in a standard currency unit without decimals.

    Author: [Your Name]
    Last Updated: [Last Update Date]
    Change Log:
    - [Date]: Initial table creation.
    - [Date]: Added 'price' column to track sale prices.
    - [Date]: Extended 'client' column length from 30 to 50 characters.
*/
(
    client        VARCHAR(50),
    purchase_date DATE,
    product       VARCHAR(50),
    price         INT
);