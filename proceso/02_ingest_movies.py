# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Ingesta Movies.csv → bronze.movies
# MAGIC Datos principales de cada película: título, géneros, idioma, puntuación, duración, fecha de estreno y votos.
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
    StructField("title", StringType(), True),
    StructField("genres", StringType(), True),
    StructField("language", StringType(), True),
    StructField("user_score", DoubleType(), True),
    StructField("runtime_hour", IntegerType(), True),
    StructField("runtime_min", IntegerType(), True),
    StructField("release_date", StringType(), True),
    StructField("vote_count", IntegerType(), True)
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
    .load(f"{ruta_raw}/Movies.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Columnas de auditoría y validación

# COMMAND ----------

df_bronze = (
    df_raw
    .withColumn("fecha_ingesta", F.current_timestamp())
    .withColumn("archivo_origen", F.col("_metadata.file_path"))
    .select("id", "title", "genres", "language", "user_score", "runtime_hour", "runtime_min", "release_date", "vote_count", "fecha_ingesta", "archivo_origen")
)

total_filas = df_bronze.count()
ids_nulos = df_bronze.filter(F.col("id").isNull()).count()

print(f"Filas leídas: {total_filas} | ids nulos: {ids_nulos}")
if total_filas == 0:
    raise ValueError("El archivo Movies.csv no tiene filas")
if ids_nulos > 0:
    raise ValueError(f"Hay {ids_nulos} filas con id nulo en Movies.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load en bronze

# COMMAND ----------

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(f"{catalogo}.bronze.movies")
)

dbutils.jobs.taskValues.set(key="filas_movies", value=total_filas)
display(spark.table(f"{catalogo}.bronze.movies").limit(10))
