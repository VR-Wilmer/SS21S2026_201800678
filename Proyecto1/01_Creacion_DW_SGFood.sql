-- =========================================================================
-- PROYECTO 1 - DATA WAREHOUSE SG-FOOD
-- =========================================================================

USE master;
GO

-- 1. Crear la base de datos si no existe
IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = N'SGFoodDW')
BEGIN
    CREATE DATABASE SGFoodDW;
END
GO

USE SGFoodDW;
GO

-- 2. Limpieza de tablas (Eliminación en cascada para evitar errores)
IF OBJECT_ID('dbo.Fact_Ventas', 'U') IS NOT NULL 
BEGIN
    ALTER TABLE dbo.Fact_Ventas DROP CONSTRAINT IF EXISTS FK_FactVentas_Cliente;
    ALTER TABLE dbo.Fact_Ventas DROP CONSTRAINT IF EXISTS FK_FactVentas_Producto;
    ALTER TABLE dbo.Fact_Ventas DROP CONSTRAINT IF EXISTS FK_FactVentas_Tiempo;
    DROP TABLE dbo.Fact_Ventas;
END

IF OBJECT_ID('dbo.Dim_Cliente', 'U') IS NOT NULL DROP TABLE dbo.Dim_Cliente;
IF OBJECT_ID('dbo.Dim_Producto', 'U') IS NOT NULL DROP TABLE dbo.Dim_Producto;
IF OBJECT_ID('dbo.Dim_Tiempo', 'U') IS NOT NULL DROP TABLE dbo.Dim_Tiempo;

-- =========================================================================
-- 3. CREACIÓN DE DIMENSIONES (Modelo Estrella)
-- =========================================================================

CREATE TABLE Dim_Cliente (
    SK_Cliente INT IDENTITY(1,1) PRIMARY KEY, 
    ClienteId VARCHAR(50) NOT NULL,                   
    ClienteNombre VARCHAR(200),
    SegmentoCliente VARCHAR(100)
);

CREATE TABLE Dim_Producto (
    SK_Producto INT IDENTITY(1,1) PRIMARY KEY,
    ProductoSKU VARCHAR(50) NOT NULL,
    ProductoNombre VARCHAR(200),
    Categoria VARCHAR(100),
    Marca VARCHAR(100)
);

CREATE TABLE Dim_Tiempo (
    SK_Fecha INT PRIMARY KEY,                 
    Fecha DATE NOT NULL,
    Anio INT,
    Mes INT,
    NombreMes VARCHAR(20),
    Dia INT
);

-- =========================================================================
-- 4. CREACIÓN DE LA TABLA DE HECHOS
-- =========================================================================

CREATE TABLE Fact_Ventas (
    SK_Venta INT IDENTITY(1,1) PRIMARY KEY,
    TransaccionId VARCHAR(50) NOT NULL,               
    SK_Fecha INT NOT NULL,
    SK_Cliente INT NOT NULL,
    SK_Producto INT NOT NULL,
    CantidadVendida INT,
    PrecioUnitario DECIMAL(18,2),
    ImporteNeto DECIMAL(18,2),
    
    CONSTRAINT FK_FactVentas_Tiempo FOREIGN KEY (SK_Fecha) REFERENCES Dim_Tiempo(SK_Fecha),
    CONSTRAINT FK_FactVentas_Cliente FOREIGN KEY (SK_Cliente) REFERENCES Dim_Cliente(SK_Cliente),
    CONSTRAINT FK_FactVentas_Producto FOREIGN KEY (SK_Producto) REFERENCES Dim_Producto(SK_Producto)
);
GO

-- =========================================================================
-- 5. POBLACIÓN INICIAL DE DIMENSIÓN TIEMPO (2020 - 2030)
-- =========================================================================

DECLARE @FechaInicio DATE = '2020-01-01';
DECLARE @FechaFin DATE = '2030-12-31';

WHILE @FechaInicio <= @FechaFin
BEGIN
    IF NOT EXISTS (SELECT 1 FROM Dim_Tiempo WHERE SK_Fecha = CONVERT(INT, CONVERT(VARCHAR(8), @FechaInicio, 112)))
    BEGIN
        INSERT INTO Dim_Tiempo (SK_Fecha, Fecha, Anio, Mes, NombreMes, Dia)
        VALUES (
            CONVERT(INT, CONVERT(VARCHAR(8), @FechaInicio, 112)), 
            @FechaInicio,
            YEAR(@FechaInicio),
            MONTH(@FechaInicio),
            DATENAME(MONTH, @FechaInicio),
            DAY(@FechaInicio)
        );
    END
    SET @FechaInicio = DATEADD(DAY, 1, @FechaInicio);
END;
GO