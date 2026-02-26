# Práctica 1 - Proceso ETL Vuelos
**Estudiante:** Wilmer Estuardo Vasquez Raxon - 201800678

## Descripción del Proceso
Se desarrolló un script en Python que realiza:
1. **Extracción:** Lectura de `dataset_vuelos_crudo.csv`.
2. **Transformación:** - Homologación de formatos de fecha.
   - Limpieza de caracteres especiales en precios.
   - Normalización de géneros y códigos de aeropuerto.
3. **Carga:** Inserción en un modelo estrella en SQL Server.

## Instrucciones de Ejecución
1. Ejecutar `creacion_tablas.sql` en SQL Server.
2. Instalar librerías: `pip install pandas pyodbc`.
3. Ejecutar `python main.py`.
4. Verificar con `consultas_analiticas.sql`.