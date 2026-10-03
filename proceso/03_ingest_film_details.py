# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Ingesta FilmDetails.csv → bronze.film_details
# MAGIC Director, reparto principal, presupuesto e ingresos (USD) de cada película.
# MAGIC
# MAGIC Lee el CSV desde la capa **raw** (ADLS, vía external location con Managed Identity), aplica un esquema explícito y agrega columnas de auditoría.

# COMMAND ----------

dbutils.widgets.text("entorno", "dev", "Entorno (dev/prod)")
dbutils.widgets.text("storage_account", "adlspeliculas2003", "Storage account")

entorno = dbutils.widgets.get("entorno").strip().lower()
storage_account = dbutils.widgets.get("storage_account").strip()

if entorno not in ("dev", "prod"):
    raise ValueError(f"Entorno no válido: {entorno}")

catalogo = f"peliculas_{entorno}"
ruta_raw = f"abfss://raw@{storage_account}.dfs.core.windows.net/peliculas"

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, LongType, DoubleType, StringType

esquema = StructType([
    StructField("id", IntegerType(), True),
    StructField("director", StringType(), True),
    StructField("top_billed", StringType(), True),
    StructField("budget_usd", LongType(), True),
    StructField("revenue_usd", LongType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract

# COMMAND ----------

df_raw = (
    spark.read.format("csv")
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')  # el archivo escapa comillas internas como ""
    .option("mode", "PERMISSIVE")
    .schema(esquema)
    .load(f"{ruta_raw}/FilmDetails.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Columnas de auditoría y validación

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("fecha_ingesta", F.current_timestamp())
    .withColumn("archivo_origen", F.col("_metadata.file_path"))
    .select("id", "director", "top_billed", "budget_usd", "revenue_usd", "fecha_ingesta", "archivo_origen")
)

total_filas = df_bronze.count()
ids_nulos = df_bronze.filter(F.col("id").isNull()).count()

print(f"Filas leídas: {total_filas} | ids nulos: {ids_nulos}")
if total_filas == 0:
    raise ValueError("El archivo FilmDetails.csv no tiene filas")
if ids_nulos > 0:
    raise ValueError(f"Hay {ids_nulos} filas con id nulo en FilmDetails.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load en bronze

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalogo}.bronze.film_details")
)

dbutils.jobs.taskValues.set(key="filas_film_details", value=total_filas)
display(spark.table(f"{catalogo}.bronze.film_details").limit(10))
