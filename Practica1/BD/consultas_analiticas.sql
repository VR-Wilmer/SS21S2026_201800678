USE Practica1_Vuelos;

-- 1. Total de vuelos cargados
SELECT COUNT(*) AS TotalVuelos FROM Fact_Vuelos;

-- 2. Top 5 de aerolíneas con más vuelos
SELECT TOP 5 A.airline_name, COUNT(F.vuelo_sk) AS CantidadVuelos
FROM Fact_Vuelos F
JOIN Dim_Aerolinea A ON F.aerolinea_sk = A.aerolinea_sk
GROUP BY A.airline_name
ORDER BY CantidadVuelos DESC;

-- 3. Distribución de pasajeros por género
SELECT P.passenger_gender, COUNT(*) AS Total
FROM Dim_Pasajero P
GROUP BY P.passenger_gender;

-- 4. Top 5 destinos más frecuentes
SELECT TOP 5 D.airport_code AS Destino, COUNT(*) AS CantidadVuelos
FROM Fact_Vuelos F
JOIN Dim_Aeropuerto D ON F.destino_aeropuerto_sk = D.aeropuerto_sk
GROUP BY D.airport_code
ORDER BY CantidadVuelos DESC;

-- 5. Vuelos por estado
SELECT F.status, COUNT(*) AS Total
FROM Fact_Vuelos F
GROUP BY F.status
ORDER BY Total DESC;

-- 6. Vuelos por año y mes
SELECT T.anio, T.mes, COUNT(*) AS TotalVuelos
FROM Fact_Vuelos F
JOIN Dim_Tiempo T ON F.fecha_vuelo_sk = T.fecha_sk
GROUP BY T.anio, T.mes
ORDER BY T.anio, T.mes;