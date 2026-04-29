CREATE OR REPLACE MODEL `proyecto-2-seminario-sistemas.taxi_data.modelo_prediccion_tarifa`
OPTIONS(
  model_type='linear_reg',
  input_label_cols=['total_amount'],
  data_split_method='CUSTOM',
  data_split_col='is_eval',
  max_iterations=10
) AS
SELECT
  total_amount,
  trip_distance,
  CAST(pickup_location_id AS STRING) AS pickup_location_id,
  CAST(dropoff_location_id AS STRING) AS dropoff_location_id,
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
  EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_day,
  CASE WHEN EXTRACT(MONTH FROM pickup_datetime) = 11 THEN TRUE ELSE FALSE END AS is_eval
FROM
  `proyecto-2-seminario-sistemas.taxi_data.taxi_trips_2022_optimized`
WHERE
  total_amount BETWEEN 1 AND 300
  AND trip_distance > 0
  AND EXTRACT(MONTH FROM pickup_datetime) IN (9, 10, 11)