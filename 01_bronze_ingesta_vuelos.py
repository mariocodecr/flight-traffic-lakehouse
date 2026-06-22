# Databricks notebook source
# MAGIC %md
# MAGIC # 🛩️ 01 · Bronze — Ingesta de vuelos (OpenSky)
# MAGIC
# MAGIC Primer paso del pipeline. Trae un *snapshot* de posiciones de aviones desde la API de
# MAGIC OpenSky y lo guarda **sin transformar** en una tabla Delta (capa **Bronze**).
# MAGIC
# MAGIC Corre las celdas **de arriba hacia abajo**, una por una.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0 · Dependencias
# MAGIC `requests` para llamar a la API. Tras instalar, reiniciamos Python.

# COMMAND ----------

# MAGIC
# MAGIC %pip install requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Configuración
# MAGIC Pega tus credenciales de OpenSky. (En el siguiente paso las moveremos a Databricks Secrets;
# MAGIC por ahora, así de simple para tener un primer resultado.)

# COMMAND ----------

# TODO: tus credenciales del API client de OpenSky
CLIENT_ID = "TU_CLIENT_ID"
CLIENT_SECRET = "TU_CLIENT_SECRET"

# Dónde se guardará la tabla. Revisa tus catálogos en la celda 2 y ajusta si "workspace" no existe.
CATALOG = "workspace"
SCHEMA = "flights_project"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_flights"

# Bounding box (lat_min, lat_max, lon_min, lon_max).
# Norteamérica = mucho tráfico, ideal para probar. Centroamérica = (5, 20, -95, -75).
BBOX = (25, 50, -125, -65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Confirmar catálogo y crear el esquema
# MAGIC Si en la lista no aparece `workspace`, cambia la variable `CATALOG` arriba por uno que sí exista
# MAGIC (por ejemplo `main`) y vuelve a correr la celda 1.

# COMMAND ----------

spark.sql("SHOW CATALOGS").display()

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Esquema listo: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Funciones para llamar a OpenSky (OAuth2)

# COMMAND ----------

import requests

TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/"
    "opensky-network/protocol/openid-connect/token"
)
STATES_URL = "https://opensky-network.org/api/states/all"


def get_token():
    r = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "client_credentials",
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        },
    )
    r.raise_for_status()
    return r.json()["access_token"]


def get_states(token, bbox):
    r = requests.get(
        STATES_URL,
        headers={"Authorization": f"Bearer {token}"},
        params={
            "lamin": bbox[0],
            "lamax": bbox[1],
            "lomin": bbox[2],
            "lomax": bbox[3],
        },
    )
    r.raise_for_status()
    return r.json()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Traer un snapshot y construir el DataFrame
# MAGIC Cada avión llega como un *array* de campos en orden fijo. Lo mapeamos a columnas con nombre
# MAGIC y agregamos dos campos de metadata: `snapshot_time` (de la API) e `ingested_at` (cuándo lo cargamos).

# COMMAND ----------

from pyspark.sql.types import (
    StructType, StructField, StringType, LongType,
    DoubleType, BooleanType, ArrayType, TimestampType,
)
from datetime import datetime, timezone

raw = get_states(get_token(), BBOX)
snapshot_time = raw["time"]
states = raw.get("states") or []
print(f"Aviones en el snapshot: {len(states)}")

schema = StructType([
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("sensors", ArrayType(LongType())),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", LongType()),
    StructField("snapshot_time", LongType()),
    StructField("ingested_at", TimestampType()),
])

now = datetime.now(timezone.utc)
rows = [tuple(s[:17]) + (snapshot_time, now) for s in states]

df = spark.createDataFrame(rows, schema=schema)
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Guardar en la tabla Bronze (Delta)
# MAGIC `mode("append")` agrega filas sin borrar lo anterior: cada corrida suma un snapshot más.

# COMMAND ----------

(
    df.write
    .format("delta")
    .mode("append")
    .saveAsTable(BRONZE_TABLE)
)

print(f"✅ Guardado en {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Verificar

# COMMAND ----------

spark.sql(f"""
    SELECT COUNT(*) AS filas,
           COUNT(DISTINCT icao24) AS aviones_distintos,
           MAX(ingested_at) AS ultima_carga
    FROM {BRONZE_TABLE}
""").display()

# COMMAND ----------

spark.sql(f"SELECT * FROM {BRONZE_TABLE} ORDER BY ingested_at DESC LIMIT 20").display()
