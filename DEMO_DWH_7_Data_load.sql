SELECT * FROM bl_3nf.ce_payment_methods;
SELECT * FROM bl_3nf.ce_promotions WHERE promotion_src_id = '14';
SELECT * FROM bl_3nf.ce_categories;
SELECT * FROM bl_3nf.ce_products;
SELECT * FROM bl_3nf.ce_cities;
SELECT * FROM bl_3nf.ce_addresses;
SELECT * FROM bl_3nf.ce_employees;
SELECT * FROM bl_3nf.ce_stores;
SELECT * FROM bl_3nf.ce_customers_scd OFFSET 10000;	
SELECT * FROM bl_3nf.ce_sales;




SELECT COUNT(*)
FROM bl_dm.fct_sales_dd
WHERE event_dt <= 20180901;


SELECT COUNT(*)
FROM bl_3nf.ce_sales
WHERE event_dt <= '2018-09-01';

SELECT * FROM bl_dm.dim_payment_methods;
SELECT * FROM bl_dm.dim_promotions;
SELECT * FROM bl_dm.dim_products;
SELECT * FROM bl_dm.dim_stores;
SELECT * FROM bl_dm.dim_customers_scd OFFSET 10000;
SELECT * FROM bl_dm.fct_sales_dd;
SELECT * FROM bl_dm.fct_sales_dd_201702;


CALL bl_cl.run_etl();

CALL bl_cl.load_data_from_ext_to_src();
CALL bl_cl.add_cities_to_t_map();

SELECT * FROM bl_cl.logging;
