CREATE OR REPLACE VIEW `proyecto-2-seminario-sistemas.taxi_data.vista_viajes_diarios` AS
SELECT
  DATE(pickup_datetime) AS fecha_viaje,
  EXTRACT(HOUR FROM pickup_datetime) AS hora_dia,
  COUNT(1) AS total_viajes,
  SUM(total_amount) AS ingreso_total
FROM
  `proyecto-2-seminario-sistemas.taxi_data.taxi_trips_2022_optimized`
GROUP BY
  fecha_viaje, hora_dia