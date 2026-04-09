-- Usar la base de datos correcta
USE SGFoodDW;
GO

-- 1. CREACIÓN DE TABLAS DE DIMENSIONES

-- Dimensión Cliente
CREATE TABLE Dim_Cliente (
    SK_Cliente INT IDENTITY(1,1) PRIMARY KEY, 
    IdCliente INT NOT NULL,                   
    NombreCliente VARCHAR(100),
    TipoCliente VARCHAR(50)
);

-- Dimensión Producto
CREATE TABLE Dim_Producto (
    SK_Producto INT IDENTITY(1,1) PRIMARY KEY,
    IdProducto INT NOT NULL,
    NombreProducto VARCHAR(100),
    CategoriaProducto VARCHAR(50),
    MarcaProducto VARCHAR(50)
);

-- Dimensión Sucursal
CREATE TABLE Dim_Sucursal (
    SK_Sucursal INT IDENTITY(1,1) PRIMARY KEY,
    IdSucursal INT NOT NULL,
    NombreSucursal VARCHAR(100),
    RegionSucursal VARCHAR(50)
);

-- Dimensión Tiempo 
CREATE TABLE Dim_Tiempo (
    SK_Fecha INT PRIMARY KEY,                 
    Fecha DATE NOT NULL,
    Anio INT,
    Mes INT,
    NombreMes VARCHAR(20),
    Dia INT
);

-- 2. CREACIÓN DE LA TABLA DE HECHOS

-- Tabla de Hechos Ventas
CREATE TABLE Fact_Ventas (
    SK_Venta INT IDENTITY(1,1) PRIMARY KEY,
    IdTransaccion INT NOT NULL,               
    SK_Fecha INT NOT NULL,
    SK_Cliente INT NOT NULL,
    SK_Producto INT NOT NULL,
    SK_Sucursal INT NOT NULL,
    MetodoPago VARCHAR(50),                   
    Cantidad INT,
    PrecioUnitario DECIMAL(18,2),
    MontoTotal DECIMAL(18,2),
    
    -- Restricciones de llaves foráneas
    CONSTRAINT FK_FactVentas_Tiempo FOREIGN KEY (SK_Fecha) REFERENCES Dim_Tiempo(SK_Fecha),
    CONSTRAINT FK_FactVentas_Cliente FOREIGN KEY (SK_Cliente) REFERENCES Dim_Cliente(SK_Cliente),
    CONSTRAINT FK_FactVentas_Producto FOREIGN KEY (SK_Producto) REFERENCES Dim_Producto(SK_Producto),
    CONSTRAINT FK_FactVentas_Sucursal FOREIGN KEY (SK_Sucursal) REFERENCES Dim_Sucursal(SK_Sucursal)
);
GO