-- query for update changes
WITH update_news AS
	(SELECT
		dph.product_id_dwh AS product_id_dwh,
		tp.product_name AS product_name,
		tp.product_price AS product_price,
		tp.product_category AS product_category,
		tp.update_ts AS update_ts
	FROM 
		public.temp_products AS tp
	JOIN
		prod_dv_dds.h_products AS dph
			ON tp.product_id = dph.product_id_bk
	LEFT JOIN 
		prod_dv_dds.s_products AS dps 
			ON dph.product_id_dwh = dps.product_id_dwh 
	WHERE 
		(tp.product_name != dps.product_name  OR tp.product_price != dps.product_price  OR tp.product_category != dps.product_category)
		AND dps.active_to = '2099-12-01'::timestamp
		AND tp.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}')
UPDATE prod_dv_dds.s_products AS dps
SET active_to = un.update_ts - INTERVAL '1 ms'
FROM update_news AS un
WHERE dps.product_id_dwh = un.product_id_dwh AND dps.active_to = '2099-12-01'::timestamp;	

--query to insert new
INSERT INTO prod_dv_dds.s_products
(product_id_dwh, product_name, product_price, product_category, active_from, active_to)
SELECT
	dph.product_id_dwh AS product_id_dwh,
	tp.product_name AS product_name,
	tp.product_price AS product_price,
	tp.product_category AS product_category,
	tp.update_ts AS active_from,
	'2099-12-01'::timestamp AS active_to
FROM 
	public.temp_products AS tp
JOIN
	prod_dv_dds.h_products AS dph
		ON tp.product_id = dph.product_id_bk
LEFT JOIN 
	(SELECT
		DISTINCT FIRST_VALUE(product_id_dwh) OVER actual_value AS product_id_dwh,
		FIRST_VALUE(product_name) OVER actual_value AS product_name,
		FIRST_VALUE(product_price) OVER actual_value AS product_price,
		FIRST_VALUE(product_category) OVER actual_value AS product_category
	FROM
		prod_dv_dds.s_products
	WINDOW actual_value AS (PARTITION BY product_id_dwh ORDER BY active_to DESC)) AS dps 
			ON dph.product_id_dwh = dps.product_id_dwh 
	WHERE 
		(tp.product_name != dps.product_name  OR tp.product_price != dps.product_price  OR tp.product_category != dps.product_category OR dps.product_name IS NULL)
		AND tp.update_ts::date = '{{ dag.timezone.convert(execution_date).strftime("%Y-%m-%d") }}'
		
