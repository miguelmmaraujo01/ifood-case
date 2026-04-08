
USE workspace.taxi;

SHOW TABLES IN workspace.taxi;

select * from taxi.silver_taxi;
--GOLD

--CREATE OR REPLACE VIEW workspace.taxi.gold_demanda_faixa_horaria AS
SELECT 
    CASE 
        WHEN HOUR(tpep_pickup_datetime) BETWEEN 0 AND 5 THEN 'Madrugada'
        WHEN HOUR(tpep_pickup_datetime) BETWEEN 6 AND 11 THEN 'Manhã'
        WHEN HOUR(tpep_pickup_datetime) BETWEEN 12 AND 17 THEN 'Tarde'
        ELSE 'Noite'
    END AS des_periodo,
    COUNT(*) AS num_qtd_corridas,
    SUM(total_amount) as val_total_periodo
FROM workspace.taxi.silver_taxi
GROUP BY des_periodo
ORDER BY num_qtd_corridas DESC;