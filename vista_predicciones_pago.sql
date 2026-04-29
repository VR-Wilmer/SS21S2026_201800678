CREATE OR REPLACE VIEW `proyecto-2-seminario-sistemas.taxi_data.vista_predicciones_pago` AS
SELECT
  pickup_datetime,
  payment_type AS metodo_real,
  predicted_is_credit_card AS prediccion_modelo,
  predicted_is_credit_card_probs[OFFSET(0)].prob AS probabilidad
FROM
  ML.PREDICT(MODEL `proyecto-2-seminario-sistemas.taxi_data.modelo_clasificacion_pago`,
    (
      SELECT 
        pickup_datetime,
        payment_type,
        total_amount,
        trip_distance,
        CAST(pickup_location_id AS STRING) AS pickup_location_id,
        EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour
      FROM `proyecto-2-seminario-sistemas.taxi_data.taxi_trips_2022_optimized` 
      WHERE EXTRACT(MONTH FROM pickup_datetime) = 11 
      LIMIT 1000
    )
  )
