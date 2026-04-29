CREATE OR REPLACE MODEL `proyecto-2-seminario-sistemas.taxi_data.modelo_clasificacion_pago`
OPTIONS(
  model_type='logistic_reg',
  input_label_cols=['is_credit_card'],
  data_split_method='CUSTOM',
  data_split_col='is_eval',
  max_iterations=10
) AS
SELECT
  CASE WHEN CAST(payment_type AS STRING) = '1' THEN 1 ELSE 0 END AS is_credit_card,
  total_amount,
  trip_distance,
  CAST(pickup_location_id AS STRING) AS pickup_location_id,
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
  CASE WHEN EXTRACT(MONTH FROM pickup_datetime) = 11 THEN TRUE ELSE FALSE END AS is_eval
FROM
  `proyecto-2-seminario-sistemas.taxi_data.taxi_trips_2022_optimized`
WHERE
  total_amount BETWEEN 1 AND 300
  AND trip_distance > 0
  AND EXTRACT(MONTH FROM pickup_datetime) IN (9, 10, 11)