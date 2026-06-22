# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 03 · Gold — Tablas de métricas
# MAGIC
# MAGIC Lee la capa **Silver** y construye tablas **agregadas y listas para el dashboard**.
# MAGIC Cada tabla responde una pregunta de negocio concreta.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Configuración

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "flights_project"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.silver_flights"

GOLD_COUNTRY = f"{CATALOG}.{SCHEMA}.gold_country_traffic"
GOLD_ALTITUDE = f"{CATALOG}.{SCHEMA}.gold_altitude_bands"
GOLD_TIME = f"{CATALOG}.{SCHEMA}.gold_traffic_by_snapshot"

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table(SILVER_TABLE)
in_air = silver.where(F.col("on_ground") == False)
print(f"Filas en Silver: {silver.count()}  |  en el aire: {in_air.count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Tráfico por país de origen
# MAGIC ¿Qué países tienen más aviones en el aire y a qué altitud/velocidad vuelan?

# COMMAND ----------

gold_country = (
    in_air
    .groupBy("origin_country")
    .agg(
        F.count("*").alias("vuelos"),
        F.round(F.avg("altitude_ft")).alias("alt_promedio_ft"),
        F.round(F.avg("velocity_kmh"), 1).alias("vel_promedio_kmh"),
    )
    .orderBy(F.desc("vuelos"))
)

gold_country.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(GOLD_COUNTRY)
print(f"✅ {GOLD_COUNTRY}")
gold_country.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Distribución por franja de altitud
# MAGIC ¿Cuántos vuelos hay en cada rango de altura? (clásico para un gráfico de barras)

# COMMAND ----------

gold_altitude = (
    in_air
    .withColumn(
        "altitude_band",
        F.when(F.col("altitude_ft") < 10000, "0-10k ft")
         .when(F.col("altitude_ft") < 20000, "10-20k ft")
         .when(F.col("altitude_ft") < 30000, "20-30k ft")
         .otherwise("30k+ ft"),
    )
    .withColumn(
        "band_order",
        F.when(F.col("altitude_ft") < 10000, 1)
         .when(F.col("altitude_ft") < 20000, 2)
         .when(F.col("altitude_ft") < 30000, 3)
         .otherwise(4),
    )
    .groupBy("altitude_band", "band_order")
    .agg(F.count("*").alias("vuelos"))
    .orderBy("band_order")
)

gold_altitude.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(GOLD_ALTITUDE)
print(f"✅ {GOLD_ALTITUDE}")
gold_altitude.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Serie temporal por snapshot
# MAGIC Aviones en el aire vs. en tierra en cada captura. Se enriquece cada vez que corres Bronze.

# COMMAND ----------

gold_time = (
    silver
    .groupBy("snapshot_ts")
    .agg(
        F.count("*").alias("aviones_total"),
        F.sum(F.when(F.col("on_ground") == False, 1).otherwise(0)).alias("en_aire"),
        F.sum(F.when(F.col("on_ground") == True, 1).otherwise(0)).alias("en_tierra"),
    )
    .orderBy("snapshot_ts")
)

gold_time.write.format("delta").mode("overwrite") \
    .option("overwriteSchema", "true").saveAsTable(GOLD_TIME)
print(f"✅ {GOLD_TIME}")
gold_time.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Resumen
# MAGIC Tus tres tablas Gold ya están listas para el dashboard:
# MAGIC - `gold_country_traffic`
# MAGIC - `gold_altitude_bands`
# MAGIC - `gold_traffic_by_snapshot`

# COMMAND ----------


spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").display()
