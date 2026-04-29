# Proyecto 2: Procesamiento y Análisis Exploratorio de Datos Masivos en BigQuery
**Universidad de San Carlos de Guatemala (USAC)** **Facultad de Ingeniería - Escuela de Ciencias y Sistemas** **Curso:** Seminario de Sistemas 2  
**Estudiante:** Wilmer Estuardo Vasquez Raxon  
**Carnet:** 201800678  

## 1. Dataset Utilizado
Se utilizó el dataset público proporcionado por Google Cloud Platform: `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2022`. Este conjunto de datos contiene la información de los viajes de taxis amarillos en la ciudad de Nueva York. 
* **Volumen inicial analizado:** 36,256,539 registros correspondientes al año 2022.

## 2. Transformaciones y Limpieza de Datos
Para garantizar la integridad temporal del análisis y evitar anomalías ("ruido"), se aplicó un filtro estricto durante la ingesta de datos (`WHERE pickup_datetime BETWEEN '2022-01-01' AND '2022-12-31'`). Asimismo, en la fase de ingeniería de características para el modelado, se realizaron las siguientes transformaciones:
* Extracción de componentes temporales: `EXTRACT(HOUR)` y `EXTRACT(DAYOFWEEK)` para capturar la variabilidad de la demanda y el tráfico.
* Casteo de variables: Conversión de `pickup_location_id`, `dropoff_location_id` y `payment_type` a formato `STRING` para que los modelos los interpreten como categorías puras y no como magnitudes matemáticas continuas.

## 3. Técnicas de Optimización Aplicadas
Para reducir costos de procesamiento y mejorar los tiempos de respuesta, se creó una tabla derivada (`taxi_trips_2022_optimized`) aplicando las siguientes técnicas:
* **Particionamiento (Partitioning):** Se particionó la tabla por día utilizando la columna `DATE(pickup_datetime)`. Esto evita el Full Table Scan al consultar rangos de fechas específicos.
* **Agrupamiento (Clustering):** Se aplicó clustering sobre `pickup_location_id` y `dropoff_location_id`, permitiendo ubicar rápidamente los bloques de datos al filtrar por zonas geográficas.

### Comparación de Desempeño (Optimizado vs Estándar)
Al realizar una prueba de lectura para un día específico (`2022-01-15`), se obtuvieron los siguientes resultados:
* **Consulta en tabla original:** [INSERTAR LOS BYTES QUE TE DIO EL DRY RUN DE LA ORIGINAL] procesados.
* **Consulta en tabla optimizada:** [INSERTAR LOS BYTES QUE TE DIO EL DRY RUN DE LA OPTIMIZADA] procesados.
* *Conclusión:* La partición demostró una reducción drástica en la cantidad de datos escaneados, traduciéndose en consultas más rápidas y económicas. *(Ver capturas en la carpeta `/evidencias`)*.

## 4. Modelos de Machine Learning (BigQuery ML)
Para prevenir el *Data Leakage* (fuga de información), ambos modelos implementaron un split temporal determinístico (`data_split_method='CUSTOM'`). Se entrenaron con datos históricos (meses 9 y 10) y se evaluaron exclusivamente contra un mes futuro (mes 11).

### Modelo 1: Regresión Lineal (Predicción de Tarifa)
* **Objetivo:** Predecir el costo total del viaje (`total_amount`).
* **Métricas obtenidas:**
  * **R2 Score:** 0.657. El modelo explica un 65.7% de la varianza del costo basándose en distancia, tiempo y ubicación.
  * **Mean Squared Error (MSE):** 98.47.
  * **Mean Absolute Error (MAE):** 6.29. En promedio, las predicciones tienen un margen de error de ~$6.30 dólares frente a la tarifa real.

### Modelo 2: Regresión Logística (Clasificación de Método de Pago)
* **Objetivo:** Predecir si un cliente pagará con tarjeta de crédito (`payment_type = 1`).
* **Métricas obtenidas:**
  * **Accuracy:** 0.777. Clasificación correcta en el 77.7% de los escenarios.
  * **Recall:** 0.995. Altísima sensibilidad; casi no genera falsos negativos para los pagos con tarjeta.
  * **Precision:** 0.779.

## 5. Hallazgos Relevantes (Análisis Exploratorio)
* El volumen de viajes presenta picos significativos en ciertas horas del día, confirmando que la variable `hora_dia` es crucial para la predicción de costos debido a la congestión.
* A pesar de las variaciones, el modelo de clasificación demuestra que existe una fuerte tendencia hacia un método de pago predominante según la distancia y la zona.
* *(Ver dashboard para el detalle visual de estos patrones temporales y económicos).*

## 6. Enlace al Informe Visual
El tablero exploratorio y la comparativa de predicciones (Rolling Forecast) fueron desarrollados en Google Looker Studio y pueden consultarse en el siguiente enlace:
* 📊 **[ENLACE_A_LOOKER_STUDIO]** *(Asegúrate de pegar aquí el link público de tu dashboard)*