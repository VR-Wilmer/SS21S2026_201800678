# Práctica 2: Diseño de Dashboard y KPIs con Power BI

**Curso:** Seminario de Sistemas 2  
**Universidad:** Universidad de San Carlos de Guatemala  
**Estudiante:** Wilmer Estuardo Vasquez Raxon  
**Carné:** 201800678

---

## 1. Descripción del Proyecto

El objetivo de esta práctica es transformar datos transaccionales de vuelos en información estratégica mediante Power BI Desktop. Para ello, se estableció una conexión directa a una base de datos relacional en SQL Server, se construyó un modelo tabular, se definieron medidas en lenguaje DAX y se diseñó un dashboard interactivo orientado a la toma de decisiones basada en indicadores clave de desempeño (KPIs).

## 2. Modelo de Datos

Se implementó un esquema de estrella enfocado en el análisis de rendimiento logístico y financiero de vuelos.

**Estructura tabular:**

- **Tabla de hechos (`Fact_Vuelos`)**: contiene métricas transaccionales como precios de tickets, tiempos de retraso y recuento de equipaje.
- **Dimensiones:**
  - `Dim_Pasajero`: atributos demográficos de los viajeros.
  - `Dim_Aerolinea`: identificación y nombre de las compañías operadoras.
  - `Dim_Aeropuerto`: nodos de origen y destino de los vuelos.
  - `Dim_Tiempo`: jerarquía temporal para análisis por año, mes y día.

## 3. Medidas DAX Implementadas

Se desarrollaron medidas calculadas para extraer valor analítico de los datos crudos:

- **Ingresos totales**

```dax
Total Revenue = SUM(Fact_Vuelos[ticket_price_usd_est])
```

- **Promedio de retraso**

```dax
Avg Delay = AVERAGE(Fact_Vuelos[delay_min])
```

- **Estado de puntualidad (numérico)**

```dax
KPI_Estado_Puntualidad =
VAR Retraso = [Avg Delay]
RETURN
SWITCH(
    TRUE(),
    ISBLANK(Retraso), BLANK(),
    Retraso <= 15, 1, -- Optimo
    Retraso <= 30, 2, -- Aceptable
    3                 -- Critico
)
```

## 4. Indicadores Clave de Desempeño (KPIs)

El indicador principal del dashboard es la **Puntualidad Operativa**. Para una visualización clara y profesional, se aplicó una semaforización mediante reglas de formato condicional:

- **Rango Óptimo (<= 15 min):** representado en verde; indica una operación eficiente.
- **Rango Aceptable (> 15 y <= 30 min):** representado en amarillo; sugiere monitoreo preventivo.
- **Nivel Crítico (> 30 min):** representado en rojo; requiere intervención estratégica inmediata.

## 5. Visualizaciones del Dashboard

El dashboard se compone de tres paneles principales interactivos:

- **Análisis temporal:** gráfico de líneas para observar la tendencia de ingresos y retrasos a lo largo del tiempo, con capacidades de drill-down.
- **Desempeño por aerolínea:** gráfico de barras comparativo para identificar operadores con mayores índices de retraso.
- **Monitor de KPIs:** tarjetas de resumen con indicadores visuales de estado para una lectura ejecutiva rápida.

## 6. Interpretación Estratégica

Con base en el modelo, se determinó que la eficiencia operativa impacta directamente en la percepción del servicio. El análisis por jerarquías permitió identificar meses con picos de retraso críticos, lo que sugiere ajustar ventanas de mantenimiento y reforzar la asignación de personal en aeropuertos específicos durante temporadas de alta demanda.