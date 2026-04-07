
USE workspace.taxi;

SHOW TABLES IN workspace.taxi;

select * from taxi.silver_taxi;
--GOLD
-- -- 3 Qual a média de passageiros passenger\_count por cada hora do dia que pegaram táxi no mês de maio considerando todos os táxis da frota?

SELECT 
  CASE 
    WHEN HOUR(tpep_pickup_datetime) < 12 THEN 'AM'
    ELSE 'PM'
  END AS des_am_pm,
  HOUR(tpep_pickup_datetime) AS num_horas,
  AVG(passenger_count) AS val_media_passageiros_hora
FROM  taxi.silver_taxi
WHERE month = 5
GROUP BY HOUR(tpep_pickup_datetime)
order by num_horas asc


        SELECT 
            HOUR(tpep_pickup_datetime) AS num_horas,
            CASE WHEN HOUR(tpep_pickup_datetime) < 12 THEN 'AM' ELSE 'PM' END AS des_am_pm,
            AVG(passenger_count) AS val_media_passageiros_hora
        FROM """ + schema + """.silver_taxi
        WHERE month = '05'
        GROUP BY HOUR(tpep_pickup_datetime)
        ORDER BY num_horas ASC

-- des_am_pm	num_pickup_hour	val_avg_passengers
-- AM	0	1.4109309957491796
-- AM	1	1.4204000745270076
-- AM	2	1.4365560896173142
-- AM	3	1.435572199404043
-- AM	4	1.3887287667872692
-- AM	5	1.2647435213821052
-- AM	6	1.23469039388896
-- AM	7	1.2523974850223343
-- AM	8	1.2658351334401783
-- AM	9	1.2832200876073534
-- AM	10	1.3186176537556606
-- AM	11	1.333532722003798
-- PM	12	1.3481767199814683
-- PM	13	1.3559378188205349
-- PM	14	1.36124411170115
-- PM	15	1.3733870624055913
-- PM	16	1.3722308833215657
-- PM	17	1.3650235863399038
-- PM	18	1.3602928538104553
-- PM	19	1.3702823735429854
-- PM	20	1.3813946555508154
-- PM	21	1.401897793298764
-- PM	22	1.4109437439009098
-- PM	23	1.4065657094582802