# Databricks notebook source
# MAGIC %md
# MAGIC # 🧼 02 · Silver — Limpieza y calidad de datos
# MAGIC
# MAGIC Lee la capa **Bronze**, descarta registros inválidos, normaliza valores y deriva
# MAGIC columnas útiles, y guarda una tabla **Silver** lista para análisis.
# MAGIC
# MAGIC Patrón: Silver se **reconstruye limpia desde Bronze cada vez** (idempotente).

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Configuración

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "flights_project"
BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.bronze_flights"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_flights"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Leer Bronze

# COMMAND ----------

from pyspark.sql import functions as F

bronze = spark.table(BRONZE_TABLE)
total_bronze = bronze.count()
print(f"Filas en Bronze: {total_bronze}")
bronze.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Reglas de calidad de datos
# MAGIC Nos quedamos solo con registros usables: con identificador y con posición válida.
# MAGIC Medimos cuántos descartamos (esto es oro para tu README).

# COMMAND ----------

valid = (
    bronze
    .where(F.col("icao24").isNotNull())
    .where(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
    .where(F.col("latitude").between(-90, 90))
    .where(F.col("longitude").between(-180, 180))
)

valid_count = valid.count()
rejected = total_bronze - valid_count
print(f"Registros válidos                          : {valid_count}")
print(f"Registros descartados (sin posición/invál.): {rejected}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Normalizar y derivar columnas
# MAGIC - `callsign` sin espacios (vacío -> null)
# MAGIC - `event_time` como timestamp real (desde el epoch de OpenSky, en segundos)
# MAGIC - altitud en pies y velocidad en km/h (más legibles que metros y m/s)

# COMMAND ----------

silver = (
    valid
    .withColumn(
        "callsign",
        F.when(F.trim(F.col("callsign")) == "", None).otherwise(F.trim(F.col("callsign"))),
    )
    .withColumn("event_time", F.col("last_contact").cast("timestamp"))
    .withColumn("snapshot_ts", F.col("snapshot_time").cast("timestamp"))
    .withColumn("altitude_ft", F.round(F.col("baro_altitude") * 3.28084, 0))
    .withColumn("velocity_kmh", F.round(F.col("velocity") * 3.6, 1))
    .select(
        "icao24",
        "callsign",
        "origin_country",
        "latitude",
        "longitude",
        "baro_altitude",
        "altitude_ft",
        "velocity",
        "velocity_kmh",
        "true_track",
        "vertical_rate",
        "on_ground",
        "event_time",
        "snapshot_ts",
        "ingested_at",
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Quitar duplicados
# MAGIC Un mismo avión puede repetirse si corriste Bronze varias veces muy seguidas.
# MAGIC Nos quedamos con una fila por (avión, momento de contacto).

# COMMAND ----------

silver = silver.dropDuplicates(["icao24", "event_time"])
print(f"Filas finales en Silver: {silver.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Guardar Silver (Delta)

# COMMAND ----------

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
print(f"✅ Guardado en {SILVER_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Verificar

# COMMAND ----------

spark.sql(f"SELECT * FROM {SILVER_TABLE} ORDER BY event_time DESC LIMIT 20").display()

# COMMAND ----------

# MAGIC %md
# MAGIC Un primer vistazo analítico: vuelos en el aire por país de origen y altitud promedio.

# COMMAND ----------

spark.sql(f"""
    SELECT origin_country,
           COUNT(*)              AS vuelos,
           ROUND(AVG(altitude_ft)) AS alt_promedio_ft
    FROM {SILVER_TABLE}
    WHERE on_ground = false
    GROUP BY origin_country
    ORDER BY vuelos DESC
""").display()
