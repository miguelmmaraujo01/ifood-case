
USE workspace.taxi;

SHOW TABLES IN workspace.taxi;

select * from taxi.silver_taxi;
--GOLD
--2 - Qual a média de valor total total\_amount recebido em um mês considerando todos os yellow táxis da frota?
--SELECT * FROM silver_taxi limit 10 

-- select * from taxi.silver_taxi;

SELECT 
  month as des_mes,
  AVG(total_amount) AS val_media_total_mes
FROM taxi.silver_taxi
GROUP BY month
ORDER BY month;

    --spark.sql("""
       -- CREATE OR REPLACE VIEW """ + schema + """.gold_media_valor_total_mes AS
    -- SELECT 
    --     month AS des_mes,
    --     AVG(total_amount) AS val_media_total_mes
    -- FROM """ + schema + """.silver_taxi
    -- GROUP BY month
    -- ORDER BY des_mes ASC
   -- """)


-- -- des_month	val_avg_total_amount
-- -- 1	27.02038310708492
-- -- 2	26.898484499532195
-- -- 3	27.803426281277332
-- -- 4	28.269516727673878
-- -- 5	28.962981777556617

-- -----------------------------------------------------------------------------------------
