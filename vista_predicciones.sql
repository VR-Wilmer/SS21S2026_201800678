CREATE OR REPLACE VIEW `proyecto-2-seminario-sistemas.taxi_data.vista_predicciones_tarifa` AS
SELECT
  pickup_datetime,
  total_amount AS tarifa_real,
  predicted_total_amount AS tarifa_predicha,
  ABS(total_amount - predicted_total_amount) AS error_absoluto
FROM
  ML.PREDICT(MODEL `proyecto-2-seminario-sistemas.taxi_data.modelo_prediccion_tarifa`,
    (
      SELECT 
        pickup_datetime,
        total_amount,
        trip_distance,
        CAST(pickup_location_id AS STRING) AS pickup_location_id,
        CAST(dropoff_location_id AS STRING) AS dropoff_location_id,
        EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
        EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_day
      FROM `proyecto-2-seminario-sistemas.taxi_data.taxi_trips_2022_optimized` 
      WHERE EXTRACT(MONTH FROM pickup_datetime) = 11 
      LIMIT 1000
    )
  )