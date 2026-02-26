import pandas as pd
import pyodbc
from pathlib import Path

# CONFIGURACIÓN DE CONEXIÓN (Cambia el 'Server' por el nombre de tu PC)
CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "Server=LOCALHOST;" 
    "Database=Practica1_Vuelos;"
    "Trusted_Connection=yes;"
)


def to_none_if_nan(value):
    if pd.isna(value):
        return None
    return value


def to_int(value, default=0):
    if pd.isna(value):
        return default
    return int(value)


def to_float(value, default=None):
    if pd.isna(value):
        return default
    return float(value)


def to_str(value, upper=False):
    if pd.isna(value):
        return None
    text = str(value).strip()
    if text == "":
        return None
    return text.upper() if upper else text


def print_table(title, columns, rows):
    str_rows = [["" if value is None else str(value) for value in row] for row in rows]
    widths = [len(str(col)) for col in columns]

    for row in str_rows:
        for idx, cell in enumerate(row):
            widths[idx] = max(widths[idx], len(cell))

    separator = "+" + "+".join("-" * (w + 2) for w in widths) + "+"

    print(f"\n{title}")
    print(separator)
    print("| " + " | ".join(str(columns[i]).ljust(widths[i]) for i in range(len(columns))) + " |")
    print(separator)

    if not str_rows:
        print("| " + " | ".join("".ljust(widths[i]) for i in range(len(columns))) + " |")
    else:
        for row in str_rows:
            print("| " + " | ".join(row[i].ljust(widths[i]) for i in range(len(columns))) + " |")

    print(separator)


def ejecutar_consultas_analiticas(conn):
    consultas = [
        (
            "1) Total de vuelos cargados",
            "SELECT COUNT(*) AS TotalVuelos FROM Fact_Vuelos"
        ),
        (
            "2) Top 5 de aerolíneas con más vuelos",
            """
            SELECT TOP 5 A.airline_name, COUNT(F.vuelo_sk) AS CantidadVuelos
            FROM Fact_Vuelos F
            JOIN Dim_Aerolinea A ON F.aerolinea_sk = A.aerolinea_sk
            GROUP BY A.airline_name
            ORDER BY CantidadVuelos DESC
            """
        ),
        (
            "3) Distribución de pasajeros por género",
            """
            SELECT P.passenger_gender, COUNT(*) AS Total
            FROM Dim_Pasajero P
            GROUP BY P.passenger_gender
            """
        ),
        (
            "4) Top 5 destinos más frecuentes",
            """
            SELECT TOP 5 D.airport_code AS Destino, COUNT(*) AS CantidadVuelos
            FROM Fact_Vuelos F
            JOIN Dim_Aeropuerto D ON F.destino_aeropuerto_sk = D.aeropuerto_sk
            GROUP BY D.airport_code
            ORDER BY CantidadVuelos DESC
            """
        ),
        (
            "5) Vuelos por estado",
            """
            SELECT F.status, COUNT(*) AS Total
            FROM Fact_Vuelos F
            GROUP BY F.status
            ORDER BY Total DESC
            """
        ),
        (
            "6) Vuelos por año y mes",
            """
            SELECT T.anio, T.mes, COUNT(*) AS TotalVuelos
            FROM Fact_Vuelos F
            JOIN Dim_Tiempo T ON F.fecha_vuelo_sk = T.fecha_sk
            GROUP BY T.anio, T.mes
            ORDER BY T.anio, T.mes
            """
        )
    ]

    print("\n--- Consultas Analíticas ---")
    cursor = conn.cursor()

    for title, query in consultas:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
        print_table(title, columns, rows)

    cursor.close()

def etl_process():
    print("--- Iniciando Proceso ETL ---")
    
    # 1. EXTRACCIÓN
    base_dir = Path(__file__).resolve().parent
    csv_path = base_dir / 'dataset_vuelos_crudo.csv'

    if not csv_path.exists():
        raise FileNotFoundError(f"No se encontró el archivo CSV en: {csv_path}")

    df = pd.read_csv(csv_path)
    print(f"Datos extraídos: {len(df)} registros.")

    # 2. TRANSFORMACIÓN (Limpieza)
    
    # Limpiar precios: Quitar comas y convertir a número
    df['ticket_price'] = df['ticket_price'].astype(str).str.replace(',', '.')
    df['ticket_price'] = pd.to_numeric(df['ticket_price'], errors='coerce')

    # Estandarizar Fechas
    for col in ['departure_datetime', 'arrival_datetime']:
        df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')

    # Estandarizar Género (M, Masculino, m -> Masculino)
    df['passenger_gender'] = df['passenger_gender'].str.strip().str.upper()
    df['passenger_gender'] = df['passenger_gender'].replace({'M': 'MASCULINO', 'F': 'FEMENINO'})

    # Manejo de Nulos: Edad (llenar con promedio) y Duración (llenar con 0)
    df['passenger_age'] = df['passenger_age'].fillna(df['passenger_age'].mean()).astype(int)
    df['duration_min'] = df['duration_min'].fillna(0)
    df['delay_min'] = df['delay_min'].fillna(0)
    df['bags_total'] = df['bags_total'].fillna(0)
    df['bags_checked'] = df['bags_checked'].fillna(0)

    # Aeropuertos a Mayúsculas
    df['origin_airport'] = df['origin_airport'].str.upper()
    df['destination_airport'] = df['destination_airport'].str.upper()

    # Normalizar texto opcional
    text_cols = [
        'airline_code', 'airline_name', 'flight_number', 'origin_airport', 'destination_airport',
        'status', 'aircraft_type', 'cabin_class', 'seat', 'passenger_id', 'passenger_gender',
        'passenger_nationality', 'sales_channel', 'payment_method'
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            df.loc[df[col].str.lower().isin(['nan', 'none', '']), col] = None

    # 3. CARGA A SQL SERVER
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    print("Limpiando tablas destino (full refresh)...")
    cursor.execute("DELETE FROM Fact_Vuelos")
    cursor.execute("DELETE FROM Dim_Tiempo")
    cursor.execute("DELETE FROM Dim_Aeropuerto")
    cursor.execute("DELETE FROM Dim_Aerolinea")
    cursor.execute("DELETE FROM Dim_Pasajero")
    cursor.execute("DBCC CHECKIDENT ('Dim_Pasajero', RESEED, 0)")
    cursor.execute("DBCC CHECKIDENT ('Dim_Aerolinea', RESEED, 0)")
    cursor.execute("DBCC CHECKIDENT ('Dim_Aeropuerto', RESEED, 0)")
    cursor.execute("DBCC CHECKIDENT ('Fact_Vuelos', RESEED, 0)")
    conn.commit()

    print("Cargando datos a la base de datos...")
    vuelos_insertados = 0
    filas_omitidas = 0
    errores = []

    aerolinea_cache = {}
    pasajero_cache = {}
    aeropuerto_cache = {}
    tiempo_cache = set()

    for idx, row in df.iterrows():
        try:
            departure_ts = to_none_if_nan(row['departure_datetime'])
            if departure_ts is None:
                filas_omitidas += 1
                continue

            departure_dt = departure_ts.to_pydatetime()
            arrival_ts = to_none_if_nan(row['arrival_datetime'])
            arrival_dt = arrival_ts.to_pydatetime() if arrival_ts is not None else None

            airline_code = to_str(row['airline_code'], upper=True)
            airline_name = to_str(row['airline_name'])
            passenger_id = to_str(row['passenger_id'])
            origin_code = to_str(row['origin_airport'], upper=True)
            destination_code = to_str(row['destination_airport'], upper=True)

            if not all([airline_code, passenger_id, origin_code, destination_code]):
                filas_omitidas += 1
                continue

            if airline_code in aerolinea_cache:
                aerolinea_sk = aerolinea_cache[airline_code]
            else:
                cursor.execute(
                    "INSERT INTO Dim_Aerolinea (airline_code, airline_name) OUTPUT INSERTED.aerolinea_sk VALUES (?, ?)",
                    airline_code, airline_name
                )
                aerolinea_sk = cursor.fetchone()[0]
                aerolinea_cache[airline_code] = aerolinea_sk

            if passenger_id in pasajero_cache:
                pasajero_sk = pasajero_cache[passenger_id]
            else:
                cursor.execute(
                    "INSERT INTO Dim_Pasajero (passenger_id, passenger_gender, passenger_age, passenger_nationality) "
                    "OUTPUT INSERTED.pasajero_sk VALUES (?, ?, ?, ?)",
                    passenger_id,
                    to_str(row['passenger_gender'], upper=True),
                    to_int(row['passenger_age'], default=None),
                    to_str(row['passenger_nationality'], upper=True)
                )
                pasajero_sk = cursor.fetchone()[0]
                pasajero_cache[passenger_id] = pasajero_sk

            if origin_code in aeropuerto_cache:
                origen_aeropuerto_sk = aeropuerto_cache[origin_code]
            else:
                cursor.execute(
                    "INSERT INTO Dim_Aeropuerto (airport_code) OUTPUT INSERTED.aeropuerto_sk VALUES (?)",
                    origin_code
                )
                origen_aeropuerto_sk = cursor.fetchone()[0]
                aeropuerto_cache[origin_code] = origen_aeropuerto_sk

            if destination_code in aeropuerto_cache:
                destino_aeropuerto_sk = aeropuerto_cache[destination_code]
            else:
                cursor.execute(
                    "INSERT INTO Dim_Aeropuerto (airport_code) OUTPUT INSERTED.aeropuerto_sk VALUES (?)",
                    destination_code
                )
                destino_aeropuerto_sk = cursor.fetchone()[0]
                aeropuerto_cache[destination_code] = destino_aeropuerto_sk

            fecha_sk = int(departure_dt.strftime('%Y%m%d'))
            if fecha_sk not in tiempo_cache:
                cursor.execute(
                    "INSERT INTO Dim_Tiempo (fecha_sk, fecha, anio, mes, dia, dia_semana) VALUES (?, ?, ?, ?, ?, ?)",
                    fecha_sk,
                    departure_dt.date(),
                    departure_dt.year,
                    departure_dt.month,
                    departure_dt.day,
                    departure_dt.strftime('%A').upper()
                )
                tiempo_cache.add(fecha_sk)

            cursor.execute("""
                INSERT INTO Fact_Vuelos (
                    pasajero_sk,
                    aerolinea_sk,
                    origen_aeropuerto_sk,
                    destino_aeropuerto_sk,
                    fecha_vuelo_sk,
                    flight_number,
                    aircraft_type,
                    cabin_class,
                    seat,
                    sales_channel,
                    payment_method,
                    status,
                    departure_datetime,
                    arrival_datetime,
                    duration_min,
                    delay_min,
                    ticket_price_usd_est,
                    bags_total,
                    bags_checked
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pasajero_sk,
                aerolinea_sk,
                origen_aeropuerto_sk,
                destino_aeropuerto_sk,
                fecha_sk,
                to_str(row['flight_number'], upper=True),
                to_str(row['aircraft_type'], upper=True),
                to_str(row['cabin_class'], upper=True),
                to_str(row['seat'], upper=True),
                to_str(row['sales_channel'], upper=True),
                to_str(row['payment_method'], upper=True),
                to_str(row['status'], upper=True),
                departure_dt,
                arrival_dt,
                to_int(row['duration_min'], default=0),
                to_int(row['delay_min'], default=0),
                to_float(row['ticket_price_usd_est'], default=None),
                to_int(row['bags_total'], default=0),
                to_int(row['bags_checked'], default=0)
            ))
            vuelos_insertados += 1

        except Exception as e:
            filas_omitidas += 1
            if len(errores) < 15:
                record_id = row['record_id'] if 'record_id' in row else idx + 1
                errores.append(f"record_id={record_id}: {str(e)}")

    conn.commit()
    cursor.close()
    print(f"Vuelos insertados: {vuelos_insertados}")
    print(f"Filas omitidas: {filas_omitidas}")
    if errores:
        print("Primeros errores detectados:")
        for err in errores:
            print(f"  - {err}")

    ejecutar_consultas_analiticas(conn)
    conn.close()
    print("--- ETL Finalizado con Éxito ---")

if __name__ == "__main__":
    etl_process()