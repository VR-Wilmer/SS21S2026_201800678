# Proyecto 1: Implementación de Solución de Inteligencia de Negocios - SG-Food

**Estudiante:** Wilmer Estuardo Vasquez Raxon  
**Carné:** 201800678  

---

## Descripción del Proyecto
Este proyecto documenta el diseño, desarrollo e implementación de una solución integral de Inteligencia de Negocios (BI) para el análisis de transacciones de ventas e inventarios. El objetivo principal es consolidar información proveniente de múltiples fuentes heterogéneas (base de datos relacional en la nube y archivos planos de Excel) en un único Data Warehouse centralizado, facilitando la explotación analítica de los datos mediante modelos multidimensionales.

---

## Fases del Desarrollo

### Fase 1: Diseño y Creación del Data Warehouse (SGFoodDW)
Se implementó un modelo de datos en **Esquema de Estrella (Star Schema)** sobre Microsoft SQL Server local, optimizado para consultas analíticas de alto volumen.
- **Dimensiones creadas:**
  - `Dim_Cliente`: almacena información demográfica y de segmentación de clientes.
  - `Dim_Producto`: clasifica el inventario por SKU, categoría y marca.
  - `Dim_Tiempo`: dimensión generada mediante un script automatizado para cubrir un rango histórico y futuro (2020-2030), habilitando análisis de series de tiempo.
- **Tabla de hechos:**
  - `Fact_Ventas`: centraliza las métricas del negocio (`CantidadVendida`, `PrecioUnitario`, `ImporteNeto`) y se relaciona con las dimensiones mediante llaves subrogadas (Surrogate Keys).

### Fase 2: Proceso de Extracción, Transformación y Carga (ETL)
El flujo ETL fue desarrollado con **SQL Server Integration Services (SSIS)** en el proyecto `SGFood_ETL`.
- **Extracción:** conexión concurrente al origen OLE DB en la nube (IP: 34.63.26.98) y al archivo Excel local.
- **Transformación:**
  - uso de componentes `Union All` para consolidar orígenes.
  - implementación de componentes `Sort` con eliminación de duplicados para las dimensiones.
  - generación dinámica de `SK_Fecha` con `Derived Column`, asegurando integridad referencial sin afectar el rendimiento.
  - uso de `Lookup` para cruzar llaves de negocio (`ClienteId`, `ProductoSKU`) y traducirlas a llaves subrogadas.
- **Carga:** inserción limpia hacia las tablas de destino mediante `OLE DB Destination`.

### Fase 3: Modelo Analítico Multidimensional (SSAS)
Se construyó un cubo OLAP con **SQL Server Analysis Services (SSAS)** en el proyecto `SGFood_SSAS` para lograr tiempos de respuesta inmediatos en el análisis.
- **Implementación:** configuración en una instancia con nombre (por ejemplo, `SSAS_CUBO`) para evitar conflictos de compatibilidad de versiones.
- **Jerarquías definidas:**
  - **Calendario:** `Anio` -> `NombreMes` -> `Dia`.
  - **Producto:** `Categoria` -> `Marca` -> `Producto Nombre`.
  - **Cliente:** `Segmento Cliente` -> `Cliente Nombre`.
- **Medidas base:** consolidación de las métricas de negocio para su totalización implícita.

### Fase 4: Validación y Explotación de Datos
La integridad del modelo multidimensional se validó mediante conexiones en vivo (Live Connection) desde Excel y herramientas de reportería final. Se verificó que los *drill-downs* jerárquicos operan correctamente y que las sumatorias de *Importe Neto* cuadran con el sistema transaccional de origen.

---

## Instrucciones de Ejecución

Para replicar y desplegar este proyecto en un nuevo entorno:

1. **Base de datos:** ejecutar el archivo `01_Creacion_DW_SGFood.sql` en SQL Server Management Studio para generar la base local `SGFoodDW`.
2. **Paquete ETL:** abrir la solución `SGFood_ETL` en Visual Studio. Actualizar los administradores de conexión (`Origen_Nube`, `Origen_Excel` y `Destino_DW`) para que apunten a las credenciales locales y ejecutar el paquete.
3. **Cubo OLAP:** abrir el proyecto `SGFood_SSAS`. En las propiedades del proyecto, modificar la cadena de conexión para apuntar a la instancia local de base de datos y a la instancia de destino de SSAS. Luego, hacer clic derecho en el proyecto y seleccionar **Procesar**.