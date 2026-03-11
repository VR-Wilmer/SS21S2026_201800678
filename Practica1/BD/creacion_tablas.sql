CREATE TABLE Dim_Pasajero (
    pasajero_sk INT IDENTITY(1,1) PRIMARY KEY, -- Surrogate Key
    passenger_id VARCHAR(50) NOT NULL,         -- Natural Key (UUID del CSV)
    passenger_gender VARCHAR(15),
    passenger_age INT,
    passenger_nationality VARCHAR(5)
);

-- Dimensión: Aerolíneas
CREATE TABLE Dim_Aerolinea (
    aerolinea_sk INT IDENTITY(1,1) PRIMARY KEY,
    airline_code VARCHAR(10) NOT NULL,
    airline_name VARCHAR(100)
);

-- Dimensión: Aeropuertos (Se usa tanto para Origen como para Destino)
CREATE TABLE Dim_Aeropuerto (
    aeropuerto_sk INT IDENTITY(1,1) PRIMARY KEY,
    airport_code VARCHAR(10) NOT NULL UNIQUE -- Ej: GUA, MEX, JFK
);

-- Dimensión: Tiempo (Fechas estandarizadas para el análisis temporal)
CREATE TABLE Dim_Tiempo (
    fecha_sk INT PRIMARY KEY,               -- Formato YYYYMMDD para fácil lectura
    fecha DATE NOT NULL,
    anio INT NOT NULL,
    mes INT NOT NULL,
    dia INT NOT NULL,
    dia_semana VARCHAR(20) NOT NULL
);

-- =========================================================================
-- FASE 2: CREACIÓN DE TABLA DE HECHOS (Transaccional)
-- =========================================================================

CREATE TABLE Fact_Vuelos (
    vuelo_sk INT IDENTITY(1,1) PRIMARY KEY,
    
    -- Claves Foráneas hacia las Dimensiones
    pasajero_sk INT NOT NULL,
    aerolinea_sk INT NOT NULL,
    origen_aeropuerto_sk INT NOT NULL,
    destino_aeropuerto_sk INT NOT NULL,
    fecha_vuelo_sk INT NOT NULL, -- Relacionado a la fecha de salida (departure)
    
    -- Datos Degenerados (Específicos del vuelo que no ameritan dimensión propia)
    flight_number VARCHAR(20),
    aircraft_type VARCHAR(20),
    cabin_class VARCHAR(30),
    seat VARCHAR(10),
    sales_channel VARCHAR(30),
    payment_method VARCHAR(30),
    status VARCHAR(20),
    
    -- Métricas / Hechos Cuantitativos
    departure_datetime DATETIME,
    arrival_datetime DATETIME,
    duration_min INT,
    delay_min INT,
    ticket_price_usd_est DECIMAL(10, 2), -- Precio ya estandarizado a USD
    bags_total INT,
    bags_checked INT,
    
    -- Restricciones de Integridad Referencial
    CONSTRAINT FK_Fact_Pasajero FOREIGN KEY (pasajero_sk) REFERENCES Dim_Pasajero(pasajero_sk),
    CONSTRAINT FK_Fact_Aerolinea FOREIGN KEY (aerolinea_sk) REFERENCES Dim_Aerolinea(aerolinea_sk),
    CONSTRAINT FK_Fact_Aeropuerto_Origen FOREIGN KEY (origen_aeropuerto_sk) REFERENCES Dim_Aeropuerto(aeropuerto_sk),
    CONSTRAINT FK_Fact_Aeropuerto_Destino FOREIGN KEY (destino_aeropuerto_sk) REFERENCES Dim_Aeropuerto(aeropuerto_sk),
    CONSTRAINT FK_Fact_Tiempo FOREIGN KEY (fecha_vuelo_sk) REFERENCES Dim_Tiempo(fecha_sk)
);