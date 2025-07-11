-- Lakeflow Pipeline: Streaming, Materialised view, view
-- path :s3://jpmctraining/input_files/sales

-- ingesting sales to bronze
create streaming table sales_pl as 
select * from stream read_files("s3://jpmctraining/input_files/sales",format=>"csv") ;


-- ingesting product to bronze
create streaming table products_pl as 
select * from stream read_files("s3://jpmctraining/input_files/products",format=>"csv") ;

-- ingesting product to bronze
create streaming table customers_pl as 
select * from stream read_files("s3://jpmctraining/input_files/customers",format=>"csv") ;