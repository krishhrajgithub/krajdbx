create streaming table jpmc.kraj_silver.sales_cleaned_pl
(constraint valid_orderid expect (order_id is not null) on violation drop row)
select distinct * from stream sales_pl ;

-- https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-apply-changes-into

-- Create a streaming table, then use AUTO CDC to populate it:
CREATE OR REFRESH STREAMING TABLE jpmc.kraj_silver.products_cleaned;

CREATE FLOW product_flow
AS AUTO CDC INTO
  kraj_silver.products_cleaned
FROM stream(products_pl)
  KEYS (product_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY seqNum
  COLUMNS * EXCEPT (operation, seqNum,_rescued_data)
  STORED AS SCD TYPE 1;


CREATE OR REFRESH STREAMING TABLE kraj_silver.customers_cleaned;

CREATE FLOW customer_flow
AS AUTO CDC INTO
  kraj_silver.customers_cleaned
FROM stream(customers_pl)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY sequenceNum
  COLUMNS * EXCEPT (operation, sequenceNum,_rescued_data)
  STORED AS SCD TYPE 2;

